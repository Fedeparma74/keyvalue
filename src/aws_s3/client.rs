use aws_sdk_s3::config::{AsyncSleep, Sleep};
use aws_sdk_s3::primitives::SdkBody;
use aws_smithy_async::time::TimeSource;
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::http::{Headers, Request, Response};
use aws_smithy_runtime_api::{
    client::{
        http::{
            HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings,
            SharedHttpConnector,
        },
        orchestrator::HttpRequest,
        runtime_components::RuntimeComponents,
    },
    shared::IntoShared,
};
use core::str::FromStr;
use reqwest::header::HeaderName;
use std::time::SystemTime;

#[derive(Debug)]
pub struct TimeSourceImpl;
impl TimeSource for TimeSourceImpl {
    fn now(&self) -> SystemTime {
        #[cfg(target_arch = "wasm32")]
        let now = {
            let offset = web_time::SystemTime::now()
                .duration_since(web_time::UNIX_EPOCH)
                .unwrap();
            std::time::UNIX_EPOCH + offset
        };
        #[cfg(not(target_arch = "wasm32"))]
        let now = SystemTime::now();

        now
    }
}

#[cfg(target_arch = "wasm32")]
struct SendTimeoutFuture(gloo_timers::future::TimeoutFuture);

#[cfg(target_arch = "wasm32")]
unsafe impl Send for SendTimeoutFuture {}
#[cfg(target_arch = "wasm32")]
unsafe impl Sync for SendTimeoutFuture {}

#[cfg(target_arch = "wasm32")]
impl std::future::Future for SendTimeoutFuture {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        use futures::FutureExt;

        self.0.poll_unpin(cx)
    }
}

#[derive(Debug, Clone)]
pub struct SleepImpl;
impl AsyncSleep for SleepImpl {
    fn sleep(&self, duration: std::time::Duration) -> Sleep {
        #[cfg(target_arch = "wasm32")]
        let sleep = SendTimeoutFuture(gloo_timers::future::sleep(duration));
        #[cfg(not(target_arch = "wasm32"))]
        let sleep = tokio::time::sleep(duration);

        Sleep::new(sleep)
    }
}

trait MakeRequest {
    async fn send(req: Request) -> Result<Response<SdkBody>, reqwest::Error>;
}

pub struct ReqwestHttpClient;

impl MakeRequest for ReqwestHttpClient {
    async fn send(req: Request) -> Result<Response<SdkBody>, reqwest::Error> {
        let client = reqwest::Client::new();

        let mut headers_map = reqwest::header::HeaderMap::new();

        let headers = req.headers().clone();
        let body = req.body().bytes().unwrap().to_vec();

        for (name, value) in headers.iter() {
            let name = name.to_string();
            let value = value.to_string();
            headers_map.insert(HeaderName::from_str(&name).unwrap(), value.parse().unwrap());
        }

        let res = client
            .request(req.method().parse().unwrap(), req.uri())
            .headers(headers_map)
            .body(body)
            .send()
            .await?;

        let status = res.status();
        let headers = res.headers().clone();
        let body = res.bytes().await?.to_vec();

        let mut response_headers = Headers::new();
        for (name, value) in headers {
            if let Some(name) = name {
                let value = value.to_str().unwrap().to_string();
                response_headers.insert(name.to_string(), value);
            }
        }

        let mut response = Response::new(status.into(), SdkBody::from(body));
        *response.headers_mut() = response_headers;

        Ok(response)
    }
}

#[derive(Debug, Clone)]
pub struct HttpClientImpl;

impl HttpConnector for HttpClientImpl {
    fn call(&self, req: HttpRequest) -> HttpConnectorFuture {
        #[cfg(not(target_arch = "wasm32"))]
        let response_fut = ReqwestHttpClient::send(req);
        #[cfg(target_arch = "wasm32")]
        let response_fut = async {
            let (tx, rx) = futures::channel::oneshot::channel();

            wasm_bindgen_futures::spawn_local(async move {
                let res = ReqwestHttpClient::send(req).await;
                tx.send(res).expect("sent request to channel");
            });

            rx.await.unwrap()
        };

        HttpConnectorFuture::new(async move {
            let response = response_fut
                .await
                .map_err(|e| ConnectorError::user(Box::new(e)))?;
            Ok(response)
        })
    }
}

impl HttpClient for HttpClientImpl {
    fn http_connector(
        &self,
        _settings: &HttpConnectorSettings,
        _components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        self.clone().into_shared()
    }
}
