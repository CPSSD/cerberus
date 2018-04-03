mod abstraction_layer;
mod nfs_layer;
mod null_layer;
mod s3_layer;

pub use self::abstraction_layer::AbstractionLayer;
pub use self::nfs_layer::NFSAbstractionLayer;
pub use self::null_layer::NullAbstractionLayer;
pub use self::s3_layer::AmazonS3AbstractionLayer;
