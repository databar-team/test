const AWS = require("aws-sdk");
const AmazonS3URI = require("amazon-s3-uri");

async function readFromBucket({ Bucket, Key }) {
  const s3 = new AWS.S3({
    apiVersion: "2006-03-01"
  });
  var params = {
    Bucket,
    Key
  };
  const data = await s3.getObject(params).promise();
  return JSON.parse(data.Body.toString("utf8"));
}

async function readFromBucketUsingURI(URI) {
  const { bucket, key } = AmazonS3URI(URI);
  return await readFromBucket({
    Bucket: bucket,
    Key: key
  });
}

async function writeToBucketUsingURI(URI, obj) {
  const { bucket, key } = AmazonS3URI(URI);
  const s3 = new AWS.S3({
    apiVersion: '2006-03-01'
  });

  var params = {
    ACL: "private",
    Body: Buffer.from(JSON.stringify(obj)),
    Bucket: bucket,
    Key: key,
    ContentType: 'application/json'
  };
  const { ETag, VersionId } = await s3.putObject(params).promise();
  return {
    Bucket: bucket,
    Key: key,
    ETag,
    VersionId
  };
}

module.exports = { readFromBucketUsingURI, writeToBucketUsingURI };
