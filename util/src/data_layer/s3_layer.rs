use std::path::{Path, PathBuf};
use std::sync::Arc;
use futures::{Future, Stream};

use rusoto_core::Region;
use rusoto_s3::{S3Client, S3};
use rusoto_s3;

use errors::*;
use distributed_filesystem::LocalFileManager;
use data_layer::abstraction_layer::AbstractionLayer;

const S3_REGION: Region = Region::EuWest1;

pub struct AmazonS3AbstractionLayer {
    client: S3Client,
    bucket: String,
    local_file_manager: Arc<LocalFileManager>,
}

impl AmazonS3AbstractionLayer {
    pub fn new(bucket: String, local_file_manager: Arc<LocalFileManager>) -> Result<Self> {
        let client = S3Client::simple(S3_REGION);

        let s3 = AmazonS3AbstractionLayer {
            client,
            bucket,
            local_file_manager,
        };

        let exists = s3.bucket_exists().chain_err(
            || "Unable to check if bucket exists",
        )?;
        if !exists {
            return Err(format!("Bucket '{}' does not exist.", s3.bucket).into());
        }

        Ok(s3)
    }

    fn abstracted_path(&self, path: &Path) -> Result<String> {
        let stripped_path = {
            if path.starts_with("/") {
                path.strip_prefix("/").chain_err(
                    || "Unable to strip prefix from path",
                )?
            } else {
                path
            }
        };
        match stripped_path.to_str() {
            Some(string) => Ok(string.to_owned()),
            None => Err(
                format!("Unable to convert path '{:?}' to a String", path).into(),
            ),
        }
    }

    pub fn file_metadata(&self, path: &Path) -> Result<rusoto_s3::HeadObjectOutput> {
        let abstracted_path = self.abstracted_path(path).chain_err(
            || "Unable to convert PathBuf to a String",
        )?;

        let request = rusoto_s3::HeadObjectRequest {
            bucket: self.bucket.clone(),
            key: abstracted_path.clone(),

            if_match: None,
            if_modified_since: None,
            if_none_match: None,
            if_unmodified_since: None,
            part_number: None,
            range: None,
            request_payer: None,
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            version_id: None,
        };

        let response = self.client.head_object(&request).sync().chain_err(|| {
            format!(
                "Unable to retrieve metadata for file {:?}",
                &abstracted_path
            )
        })?;
        Ok(response)
    }

    pub fn bucket_exists(&self) -> Result<bool> {
        let result = self.client.list_buckets().sync().chain_err(
            || "Unable to retrieve bucket list",
        )?;

        match result.buckets {
            Some(buckets) => {
                for bucket in buckets {
                    if let Some(name) = bucket.name {
                        if self.bucket == name {
                            return Ok(true);
                        }
                    } else {
                        return Err("Returned bucket has no name".into());
                    }
                }
                Ok(false)
            }
            None => Err("Unable to get list of buckets".into()),
        }
    }
}

impl AbstractionLayer for AmazonS3AbstractionLayer {
    fn get_file_length(&self, path: &Path) -> Result<u64> {
        let metadata = self.file_metadata(path).chain_err(
            || "Unable to get metadata for file",
        )?;

        if let Some(size) = metadata.content_length {
            return Ok(size as u64);
        }

        Err(
            format!("Unable to get content length of file: {:?}", path).into(),
        )
    }

    fn read_file_location(&self, path: &Path, start_byte: u64, end_byte: u64) -> Result<Vec<u8>> {
        let abstracted_path = self.abstracted_path(path).chain_err(
            || "Unable to get abstracted path",
        )?;

        let request = rusoto_s3::GetObjectRequest {
            bucket: self.bucket.clone(),
            if_match: None,
            if_modified_since: None,
            if_none_match: None,
            if_unmodified_since: None,
            key: abstracted_path,
            part_number: None,
            range: None,
            request_payer: None,
            response_cache_control: None,
            response_content_disposition: None,
            response_content_encoding: None,
            response_content_language: None,
            response_content_type: None,
            response_expires: None,
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            version_id: None,
        };

        let response = self.client.get_object(&request).sync().chain_err(
            || "Unable to get object",
        )?;

        let streaming_body = match response.body {
            Some(body) => body,
            None => return Err("Object has no body".into()),
        };

        // TODO(rhino): Make this more efficient
        let result: Vec<u8> = streaming_body.concat2().wait().chain_err(
            || "Unable to get body of file",
        )?;

        let mut bytes: Vec<u8> = vec![];
        let start = start_byte as usize;
        let end = end_byte as usize;

        bytes.extend_from_slice(&result[start..end]);

        Ok(bytes)
    }

    fn write_file(&self, path: &Path, data: &[u8]) -> Result<()> {
        let abstracted_path = self.abstracted_path(path).chain_err(
            || "Unable to get abstracted path",
        )?;

        let request = rusoto_s3::PutObjectRequest {
            bucket: self.bucket.clone(),
            key: abstracted_path.clone(),

            acl: None,
            body: Some(data.to_vec()),
            cache_control: None,
            content_disposition: None,
            content_encoding: None,
            content_language: None,
            content_length: None,
            content_md5: None,
            content_type: None,
            expires: None,
            grant_full_control: None,
            grant_read: None,
            grant_read_acp: None,
            grant_write_acp: None,
            metadata: None,
            request_payer: None,
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            ssekms_key_id: None,
            server_side_encryption: None,
            storage_class: None,
            tagging: None,
            website_redirect_location: None,
        };

        self.client.put_object(&request).wait().chain_err(
            || "Unable to put object into bucket",
        )?;
        Ok(())
    }

    fn get_local_file(&self, path: &Path) -> Result<PathBuf> {
        // Return the path to the local file if we have it.
        if let Some(local_file_path) =
            self.local_file_manager.get_local_file(
                &path.to_string_lossy(),
            )
        {
            return Ok(PathBuf::from(local_file_path));
        }

        // Otherwise download the file and return it's path.
        info!("Downloading remote file: {:?}", path);
        let file_length = self.get_file_length(path).chain_err(
            || "Error getting file length",
        )?;
        let data = self.read_file_location(path, 0, file_length).chain_err(
            || "Error reading remote file",
        )?;

        let local_file_path = self.local_file_manager
            .write_local_file(&path.to_string_lossy(), &data)
            .chain_err(|| "Erro writing local file")?;
        info!("Finished downloading {:?}", path);

        Ok(PathBuf::from(local_file_path))
    }

    fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let mut abstracted_path = self.abstracted_path(path).chain_err(
            || "Unable to convert Path to a String",
        )?;
        abstracted_path = format!("{}/", abstracted_path);
        let request = rusoto_s3::ListObjectsV2Request {
            bucket: self.bucket.clone(),
            continuation_token: None,
            delimiter: None,
            encoding_type: None,
            fetch_owner: None,
            max_keys: None,
            prefix: Some(abstracted_path.clone()),
            request_payer: None,
            start_after: None,
        };

        let mut files: Vec<PathBuf> = vec![];
        let response = self.client.list_objects_v2(&request).sync().chain_err(
            || "Unable to get list of objects from bucket",
        )?;
        if let Some(contents) = response.contents {
            for object in contents {
                if let Some(file_name) = object.key {
                    if file_name == abstracted_path {
                        continue;
                    }
                    let pathbuf = PathBuf::from(file_name);
                    files.push(pathbuf);
                }
            }
        }
        Ok(files)
    }

    // TODO: Readdress this when the rusoto_s3::HeadObjectError gets fixed.
    fn is_file(&self, path: &Path) -> Result<(bool)> {
        let metadata = self.file_metadata(path);
        match metadata {
            Ok(_) => Ok(true),
            Err(err) => {
                warn!(
                    "Error occured getting file metadata: {:?}",
                    err.description()
                );
                Ok(false)
            }
        }
    }

    // S3 automatically creates folders if they don't exist, so we can just return true if the path
    // isn't a file.
    fn is_dir(&self, path: &Path) -> Result<(bool)> {
        let is_file = self.is_file(path).chain_err(
            || "Unable to check if path is a file",
        )?;
        Ok(!is_file)
    }

    fn exists(&self, path: &Path) -> Result<(bool)> {
        self.is_file(path)
    }

    // S3 automatically creates folders if they don't exist, so we can just return true here.
    fn create_dir_all(&self, _: &Path) -> Result<()> {
        Ok(())
    }

    // For S3 all files can be taken to be equally close to each worker.
    fn get_data_closeness(
        &self,
        _path: &Path,
        _chunk_start: u64,
        _chunk_end: u64,
        _worker_id: &str,
    ) -> Result<u64> {
        Ok(1)
    }
}
