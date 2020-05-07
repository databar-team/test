const { readFromBucketUsingURI, writeToBucketUsingURI } = require('./s3');
const { groupBy, uniqBy, flatten } = require('lodash');
const { findPreferredTracks } = require('./findPreferredTracks');
require("promise.allsettled").shim();

const isTrackReady = (material, isVideo, tr, tracks, trackErrors) => {
  const fileId = isVideo ? 'vid' : tr.trackTypeId;
  for (let i = 0; i < material.Material.TrackTypeLink.length; i++) {
    const track = material.Material.TrackTypeLink[i];
    if (track.TrackType.FileTag === fileId) {
      if (track.StateName === 'Ready') {
        tracks.push(track);
      } else {
        trackErrors.push(`Track Type: "${track.TrackTypeName}" with trackTypeId: "${track.TrackType.FileTag}" is not in Ready state`);
      }

      break;
    }
  }
  if (tracks.length === 0
    && trackErrors.length === 0) {
    const errorMessage = isVideo ? 'Track Type: "Video" is not found in the material' : `Track Type: "${tr.trackTypeName}" with trackTypeId: "${tr.trackTypeId}" is not found in the material`;
    trackErrors.push(errorMessage);
  }
};

const validateMaterial = (material, tracks, trackErrors, trackType, isVideo, trackTypeIds) => {
  if (isVideo) {
    isTrackReady(material, isVideo, null, tracks, trackErrors);
  } else {
    trackTypeIds[trackType].forEach((tr) => {
      isTrackReady(material, isVideo, tr, tracks, trackErrors);
    });
  }
};

const validateWorkOrderAndMaterial = (wo, material, consolidated) => {
  const { MaterialType } = material.Material;
  let audioAndCaptionPref = [];
  if (wo.requiredComponents.includes('video') && wo.audioAndCaptionPreference) {
    const conformPackage = wo.profile.packages.find(p => p.conform);
    if (conformPackage && conformPackage.conform.length) {
      conformPackage.conform
        .filter(pack => pack.audioAndCaptionHierarchy)
        .forEach((p) => {
          p.audioAndCaptionHierarchy
            .filter(hh => hh.materialType === MaterialType)
            .forEach((el) => {
              audioAndCaptionPref = el.hierarchies.filter(hh => hh.profileName === wo.audioAndCaptionPreference);
            });
        });
    } else {
      throw new Error('WorkOrder: audioAndCaptionHierarchy is invalid');
    }
    if (audioAndCaptionPref.length === 0) {
      throw new Error('WorkOrder: Embedded language requirements are not found in the workorder for the MaterialType specified in material');
    } else {
      audioAndCaptionPref.forEach((aud) => {
        aud.audioEmbeddedTracks.forEach((tr, i) => {
          if (tr.trackTypeId) {
            consolidated.trackTypeIds.audioEmbTracks.push(tr);
          } else {
            throw new Error(`WorkOrder: TrackTypeId "${tr.trackTypeId}" in audioEmbeddedTracks at index ${i} is invalid`);
          }
        });
        aud.embeddedCaptionTracks.forEach((tr, i) => {
          if (tr.trackTypeId) {
            consolidated.trackTypeIds.embeddedCapTracks.push(tr);
          } else {
            throw new Error(`WorkOrder: TrackTypeId "${tr.trackTypeId}" in embeddedCapTracks at index ${i} is invalid`);
          }
        });
      });
    }

    validateMaterial(material, consolidated.tracks.videos, consolidated.errors.videos, null, true, null);
    validateMaterial(material, consolidated.tracks.audioEmbTracks, consolidated.errors.audioEmbTracks, 'audioEmbTracks', false, consolidated.trackTypeIds);
    validateMaterial(material, consolidated.tracks.embeddedCapTracks, consolidated.errors.embeddedCapTracks, 'embeddedCapTracks', false, consolidated.trackTypeIds);
  }
  if (wo.requiredComponents.includes('cc')) {
    const closedCapPackage = wo.profile.packages.find(pack => pack.closedCaptions);
    if (closedCapPackage && closedCapPackage.closedCaptions.length) {
      closedCapPackage.closedCaptions.forEach((cc, i) => {
        if (cc.trackTypeId) {
          consolidated.trackTypeIds.closedCapTracks.push(cc);
        } else {
          throw new Error(`WorkOrder: TrackTypeId "${cc.trackTypeId}" in closedCaptions at index ${i} is invalid`);
        }
      });
    } else {
      throw new Error('WorkOrder: closedCaptions are invalid');
    }
    validateMaterial(material, consolidated.tracks.closedCaptions, consolidated.errors.closedCaptions, 'closedCapTracks', false, consolidated.trackTypeIds);
  }
};

const finalizeTracks = (consolidated, material) => {
  const validTracks = [];

  if (consolidated.tracks.videos.length) {
    validTracks.push(...findPreferredTracks(material, consolidated.tracks.videos, 'video'));
  }

  if (consolidated.tracks.audioEmbTracks.length) {
    validTracks.push(...findPreferredTracks(material, consolidated.tracks.audioEmbTracks, 'audio'));
  }

  if (consolidated.tracks.closedCaptions.length || consolidated.tracks.embeddedCapTracks.length) {
    const captions = [...consolidated.tracks.closedCaptions, ...consolidated.tracks.embeddedCapTracks];
    validTracks.push(...findPreferredTracks(material, captions, 'caption'));
  }
  const groupedValidTracks = groupBy(validTracks, 'MediaName');
  const validTracksWithoutDups = [];

  for (const [, tracks] of Object.entries(groupedValidTracks)) {
    const combinedTracks = tracks[0];
    for (let i = 1; i < tracks.length; i += 1) {
      if (tracks[i].TrackDefinition.length) {
        combinedTracks.TrackDefinition.push(tracks[i].TrackDefinition);
      }
    }
    validTracksWithoutDups.push(combinedTracks);
  }
  return validTracksWithoutDups;
};


const finalizeMaterial = (consolidated, material, validTracksWithoutDups) => {
  const trackTypeLinks = [...consolidated.tracks.videos, ...consolidated.tracks.audioEmbTracks, ...consolidated.tracks.embeddedCapTracks, ...consolidated.tracks.closedCaptions];
  delete material.Material.TrackTypeLink;
  if (trackTypeLinks.length !== 0) {
    const uniqTrackTypeLinks = uniqBy(trackTypeLinks, 'TrackType.FileTag');
    material.Material.TrackTypeLink = uniqTrackTypeLinks;
  }
  delete material.Material.Track;
  if (validTracksWithoutDups.length !== 0) {
    material.Material.Track = validTracksWithoutDups;
  }
};

const validateAndSaveMaterial = async(log, statusSender, event) => {
  const promises = [];
  const workOrderId = ((event || {}).workOrder || {}).workOrderId || "error";
  const logContext = {
    WOId: workOrderId,
    jobId: event.jobId
  };
  log.info(`Starting validation of work order`, logContext);
  const validatingStatus = "Validating Material.";
  await statusSender.send("Running", {
    workOrderId: workOrderId,
    jobId: event.jobId,
    statusMessage: validatingStatus
  });
  log.info(validatingStatus, logContext);
  let material;
  try {
    material = await readFromBucketUsingURI(event.rawMaterialMetadataInputFile);
  } catch (e) {
    const statusMessage = "materialsValid";
    await statusSender.send("Error", {
      workOrderId: workOrderId,
      jobId: event.jobId,
      statusMessage,
      errors: [
        {
          errorCode: 1,
          errorMessage: `Could not find material metadata at ${event.rawMaterialMetadataInputFile}`
        }
      ]
    });
    log.error(statusMessage, logContext);
    throw e;
  }
  if (material) {
    try {
      const wo = event.workOrder;
      const consolidated = {
        trackTypeIds: {
          audioEmbTracks: [],
          embeddedCapTracks: [],
          closedCapTracks: []
        },
        errors: {
          videos: [],
          audioEmbTracks: [],
          embeddedCapTracks: [],
          closedCaptions: [],
        },
        tracks: {
          videos: [],
          audioEmbTracks: [],
          embeddedCapTracks: [],
          closedCaptions: [],
        }
      };

      validateWorkOrderAndMaterial(wo, material, consolidated);

      const errors = flatten(Object.values(consolidated.errors));
      if (errors.length > 0) {
        throw new Error(`Material: ${errors}`)
      } else {
        const tracks = finalizeTracks(consolidated, material)
        finalizeMaterial(consolidated, material, tracks)

        log.info(`Saving material: ${material.Material.MatId} at ${event.materialMetadataOutputFile}`, logContext);

        const s3Promise = writeToBucketUsingURI(event.materialMetadataOutputFile, material).then(() =>
          statusSender.send("Done", {
            workOrderId: workOrderId,
            jobId: event.jobId,
            statusMessage: `Material ${material.Material.MatId} Successfully Validated & Saved at ${event.materialMetadataOutputFile}`
          })
        );
        promises.push(s3Promise);
      }
    } catch (e) {
      const error = {
        errorCode: 2,
        errorMessage: e.message
      };
      const statusMessage = e.message.includes("Waiting for Mediator to finish transferring") ? "Pending Transfer." : "materialsValid";      
      log.error(statusMessage, logContext);
      promises.push(
        statusSender.send("Error", {
          workOrderId: workOrderId,
          jobId: event.jobId,
          statusMessage,
          errors: [error]
        })
      );
    }
  }
  await Promise.allSettled(promises);
}


module.exports = { validateAndSaveMaterial };
