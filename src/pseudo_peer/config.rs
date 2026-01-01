use crate::chainspec::HlChainSpec;

use super::sources::{
    BlockSourceBoxed, CachedBlockSource, HlNodeBlockSource, HlNodeBlockSourceArgs,
    LocalBlockSource,
};
#[cfg(feature = "s3")]
use super::sources::S3BlockSource;
#[cfg(feature = "s3")]
use aws_config::BehaviorVersion;
use std::{env::home_dir, path::PathBuf, sync::Arc};
#[cfg(feature = "s3")]
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct BlockSourceConfig {
    pub source_type: BlockSourceType,
    pub block_source_from_node: Option<HlNodeBlockSourceArgs>,
}

#[derive(Debug, Clone)]
pub enum BlockSourceType {
    #[cfg(feature = "s3")]
    S3Default { polling_interval: Duration },
    #[cfg(feature = "s3")]
    S3 { bucket: String, polling_interval: Duration },
    Local { path: PathBuf },
}

impl BlockSourceConfig {
    #[cfg(feature = "s3")]
    pub async fn s3_default(polling_interval: Duration) -> Self {
        Self {
            source_type: BlockSourceType::S3Default { polling_interval },
            block_source_from_node: None,
        }
    }

    #[cfg(feature = "s3")]
    pub async fn s3(bucket: String, polling_interval: Duration) -> Self {
        Self {
            source_type: BlockSourceType::S3 { bucket, polling_interval },
            block_source_from_node: None,
        }
    }

    pub fn local(path: PathBuf) -> Self {
        Self { source_type: BlockSourceType::Local { path }, block_source_from_node: None }
    }

    pub fn local_default() -> Self {
        Self {
            source_type: BlockSourceType::Local {
                path: home_dir()
                    .expect("home dir not found")
                    .join("hl")
                    .join("data")
                    .join("evm_block_and_receipts"),
            },
            block_source_from_node: None,
        }
    }

    pub fn with_block_source_from_node(
        mut self,
        block_source_from_node: HlNodeBlockSourceArgs,
    ) -> Self {
        self.block_source_from_node = Some(block_source_from_node);
        self
    }

    pub async fn create_block_source(&self, _chain_spec: HlChainSpec) -> BlockSourceBoxed {
        match &self.source_type {
            #[cfg(feature = "s3")]
            BlockSourceType::S3Default { polling_interval } => {
                s3_block_source(_chain_spec.official_s3_bucket(), *polling_interval).await
            }
            #[cfg(feature = "s3")]
            BlockSourceType::S3 { bucket, polling_interval } => {
                s3_block_source(bucket, *polling_interval).await
            }
            BlockSourceType::Local { path } => {
                Arc::new(Box::new(LocalBlockSource::new(path.clone())))
            }
        }
    }

    pub async fn create_block_source_from_node(
        &self,
        next_block_number: u64,
        fallback_block_source: Option<BlockSourceBoxed>,
    ) -> BlockSourceBoxed {
        let Some(block_source_from_node) = self.block_source_from_node.as_ref() else {
            return fallback_block_source.expect("Either block_source_from_node or fallback must be provided");
        };

        Arc::new(Box::new(
            HlNodeBlockSource::new(
                fallback_block_source,
                block_source_from_node.clone(),
                next_block_number,
            )
            .await,
        ))
    }

    pub async fn create_cached_block_source(
        &self,
        #[allow(unused_variables)] chain_spec: HlChainSpec,
        next_block_number: u64,
    ) -> BlockSourceBoxed {
        #[cfg(feature = "s3")]
        let fallback = Some(self.create_block_source(chain_spec).await);
        #[cfg(not(feature = "s3"))]
        let fallback = None;

        let block_source =
            self.create_block_source_from_node(next_block_number, fallback).await;
        Arc::new(Box::new(CachedBlockSource::new(block_source)))
    }
}

#[cfg(feature = "s3")]
async fn s3_block_source(bucket: impl AsRef<str>, polling_interval: Duration) -> BlockSourceBoxed {
    let client = aws_sdk_s3::Client::new(
        &aws_config::defaults(BehaviorVersion::latest()).region("ap-northeast-1").load().await,
    );
    Arc::new(Box::new(S3BlockSource::new(client, bucket.as_ref().to_string(), polling_interval)))
}
