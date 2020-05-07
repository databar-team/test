'use strict';

const trackConfig = require('./trackConfig');

function findPreferredTracks(material, tracks, type) {
  const sourceFiles = [];
  const trackMediaNames = [];
  const stage = process.env.NODE_ENV;

  const { MatId } = material.Material;
  tracks.forEach((track) => {
    for (let i = 0; i < material.Material.Track.length; i++) {
      const { TrackDefinition, MediaName } = material.Material.Track[i];

      const index = trackConfig.M8[type].findIndex(v => v.MediaName === MediaName);
      const trackConf = trackConfig.M8[type][index];

      if (index === -1) continue;

      TrackDefinition.forEach((trackDef) => {
        if (trackDef.TrackType.FileTag === track.TrackType.FileTag) {
          const { FileId } = trackDef;
          let cpSourcePathStart;
          if (stage !== 'prod') {
            cpSourcePathStart = trackConf.preprodSourcePathStart ? trackConf.preprodSourcePathStart : trackConf.prodSourcePathStart;
          } else {
            cpSourcePathStart = trackConf.prodSourcePathStart;
          }
          const sourcePathEnd = trackConf.sourcePathEnd.replace('MatId', MatId).replace('FileId', FileId);
          const currentTrack = {
            track: {
              ...trackDef,
              'cp-sourceType'    : trackConf.sourceType,
              'cp-sourceLocation': trackConf.sourceLocation,
              'cp-sourcePath'    : `${cpSourcePathStart}${sourcePathEnd}`,
            },
            index,
            MediaName,
          };

          const existingTrackIdx = sourceFiles.findIndex(sf => sf.track.TrackType.FileTag === track.TrackType.FileTag);

          const existingTrack = sourceFiles[existingTrackIdx];

          if (existingTrack) {
            if (existingTrack.index > currentTrack.index) {

              sourceFiles.splice(existingTrackIdx, 1, currentTrack);

              if (currentTrack.MediaName !== existingTrack.MediaName) {
                const trackMediaNameIdx = trackMediaNames.findIndex(mn => mn === existingTrack.MediaName);
                trackMediaNames.splice(trackMediaNameIdx, 1, currentTrack.MediaName);
              }
            }
          } else {
            sourceFiles.push(currentTrack);
            trackMediaNames.push(currentTrack.MediaName);
          }
        }
      });
    }
  });

  const result = [];

  trackMediaNames.forEach((mn) => {
    const track = material.Material.Track.find(tr => tr.MediaName === mn);
    const newTrack = {
      ...track,
      TrackDefinition: [],
    };
    result.push(newTrack);
  });

  sourceFiles.forEach((sf) => {
    const track = result.find(tr => tr.MediaName === sf.MediaName && tr.Encoded === true);
    if (track) {
      track.TrackDefinition.push(sf.track);
    } else {
      throw new Error(`Waiting for Mediator to finish transferring ${sf.track.TrackTypeName} to ${sf.MediaName} for ${MatId}`);
    }
  });

  return result;

}

module.exports = { findPreferredTracks };
