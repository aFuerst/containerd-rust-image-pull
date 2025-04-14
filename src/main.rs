use std::collections::HashMap;
use containerd_client::services::v1::leases_client::LeasesClient;
use containerd_client::tonic::Request;
use containerd_client::services::v1::streaming_client::StreamingClient;
use containerd_client::services::v1::transfer_client::TransferClient;
use containerd_client::services::v1::{CreateRequest, TransferRequest};
use containerd_client::types::Platform;
use containerd_client::types::transfer::{ImageStore, OciRegistry, RegistryResolver, UnpackConfiguration};
use containerd_client::with_namespace;
use tonic::IntoStreamingRequest;
use uuid::Uuid;

const CONTAINERD_SOCK: &str = "/run/containerd/containerd.sock";
// const IMAGE_NAME: &str = "registry.docker.iu.edu/iluvatar-faas/hello-iluvatar-action:latest";
const IMAGE_NAME: &str = "docker.io/alfuerst/hello-iluvatar-action:latest";
const NAMESPACE: &str = "default";

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() {
    let channel = containerd_client::connect(CONTAINERD_SOCK).await.unwrap();
    let lease_name = Uuid::new_v4().to_string();

    // https://github.com/containerd/containerd/blob/main/docs/garbage-collection.md#using-grpc
    let mut lease_client = LeasesClient::new(channel.clone());
    let lease_req = with_namespace!(CreateRequest {
        id: lease_name.clone(),
        labels: HashMap::new(),
    }, NAMESPACE);
    let lease = match lease_client.create(lease_req).await {
        Ok(l) => { println!("lease OK"); l.into_inner() },
        Err(e) => {
            println!("lease create failed {:?}", e);
            return;
        }
    };
    let lease = lease.lease.unwrap();
    println!("lease: {:?}", lease);

    let stream_uuid = Uuid::new_v4().to_string();
    let mut stream_client = StreamingClient::new(channel.clone());
    let req = containerd_client::services::v1::StreamInit {
        id: stream_uuid.clone()
    };
    let req = containerd_client::to_any(&req);
    let mut req = tokio_stream::iter([req]).into_streaming_request();
    req.metadata_mut().insert("containerd-lease", lease.id.clone().parse().unwrap());
    let mut stream = match stream_client.stream(req).await {
        Ok(s) => s.into_inner(),
        Err(e) => {
            println!("stream init failed {:?}", e) ;
            return;
        }
    };
    println!("checking stream 1!");
    match stream.message().await {
        Err(e) => {
            println!("rcv stream init failed {:?}", e);
            return;
        },
        Ok(None) => println!("init stream closed?"),
        Ok(Some(val)) => println!("init stream value {:?}", val)
    };
    let trans_options = Some(containerd_client::services::v1::TransferOptions { progress_stream:stream_uuid.clone() });
    let resolver = Some(RegistryResolver {
        // auth_stream: stream_uuid.clone(),
        ..Default::default()
    });

    let source = OciRegistry {
        reference: IMAGE_NAME.to_string(),
        resolver,
    };
    let arch: String = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        _ => std::env::consts::ARCH,
    }.to_string();
    let platform = Platform {
        os: "linux".to_string(),
        architecture: arch,
        ..Default::default()
    };

    let destination = ImageStore {
        name: IMAGE_NAME.to_string(),
        platforms: vec![platform.clone()],
        unpacks: vec![UnpackConfiguration {
            platform: Some(platform),
            snapshotter: "overlayfs".to_string(),
        }],
        ..Default::default()
    };

    let anys: prost_types::Any = containerd_client::to_any(&source);
    let anyd: prost_types::Any = containerd_client::to_any(&destination);
    let request = TransferRequest {
        source: Some(anys),
        destination: Some(anyd),
        options: trans_options,
    };

    println!("sending pull request");
    let j = tokio::spawn(  async move{
        let mut client = TransferClient::new(channel.clone());
        let mut req = with_namespace!(request, NAMESPACE);
        req.metadata_mut().insert("containerd-lease", lease.id.clone().parse().unwrap());
        client.transfer(req).await
    });
    // println!("checking stream 3!");
    // match stream.message().await {
    //     Err(e) => println!("rcv stream init failed {:?}", e),
    //     Ok(None) => println!("init stream closed?"),
    //     Ok(Some(val)) => println!("init stream value {:?}", val)
    // };
    println!("checking pull result");
    match j.await.unwrap()  {
        Ok(_) => println!("pull success!"),
        Err(e) => println!("Error pulling image {:?}", e),
    }
}
