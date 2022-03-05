use std::{
    sync::Arc,
    thread::JoinHandle,
    time::{Duration, Instant},
};

use argon2::{
    password_hash::{ParamsString, SaltString},
    Algorithm, Argon2, Block, ParamsBuilder, PasswordHash, Version,
};
use bonsaidb_core::connection::SensitiveString;
use once_cell::sync::OnceCell;
use rand::{thread_rng, CryptoRng, Rng};

use crate::{
    config::{ArgonConfiguration, ArgonParams},
    Error,
};

#[derive(Debug)]
#[cfg_attr(not(test), allow(dead_code))]
pub struct Hasher {
    sender: flume::Sender<HashRequest>,
    threads: Vec<JoinHandle<()>>,
}

impl Hasher {
    pub fn new(config: ArgonConfiguration) -> Self {
        let (sender, receiver) = flume::unbounded();
        let thread = HashingThread {
            receiver,
            algorithm: config.algorithm,
            params: config.params,
            blocks: Vec::default(),
            builder_template: Arc::default(),
        };
        let mut threads = Vec::with_capacity(config.hashers as usize);
        for _ in 0..config.hashers.max(1) {
            let thread = thread.clone();
            threads.push(
                std::thread::Builder::new()
                    .name(String::from("argon2"))
                    .spawn(move || thread.process_requests())
                    .unwrap(),
            );
        }
        Hasher { sender, threads }
    }

    pub fn hash(&self, id: u64, password: SensitiveString) -> Result<SensitiveString, Error> {
        let (result_sender, result_receiver) = flume::bounded(1);
        if self
            .sender
            .send(HashRequest {
                id,
                password,
                verify_against: None,
                result_sender,
            })
            .is_ok()
        {
            match result_receiver.recv()?.map_err(Error::from) {
                Ok(HashResponse::Hash(hash)) => Ok(hash),
                Ok(HashResponse::Verified) => unreachable!(),
                Err(err) => Err(err),
            }
        } else {
            Err(Error::InternalCommunication)
        }
    }

    pub fn verify(
        &self,
        id: u64,
        password: SensitiveString,
        saved_hash: SensitiveString,
    ) -> Result<(), Error> {
        let (result_sender, result_receiver) = flume::bounded(1);
        if self
            .sender
            .send(HashRequest {
                id,
                password,
                verify_against: Some(saved_hash),
                result_sender,
            })
            .is_ok()
        {
            match result_receiver.recv()?.map_err(Error::from) {
                Ok(_) => Ok(()),
                Err(err) => {
                    eprintln!("Error validating password for user {}: {:?}", id, err);
                    Err(Error::Core(bonsaidb_core::Error::InvalidCredentials))
                }
            }
        } else {
            Err(Error::InternalCommunication)
        }
    }
}

#[derive(Clone, Debug)]
struct HashingThread {
    receiver: flume::Receiver<HashRequest>,
    algorithm: Algorithm,
    params: ArgonParams,
    blocks: Vec<Block>,
    builder_template: Arc<OnceCell<Result<ParamsBuilder, ArgonError>>>,
}

impl HashingThread {
    fn initialize_builder_template<R: Rng + CryptoRng>(
        &mut self,
        rng: &mut R,
    ) -> Result<ParamsBuilder, ArgonError> {
        match &self.params {
            ArgonParams::Params(builder) => Ok(builder.clone()),
            ArgonParams::Timed(config) => {
                let mut params_builder = ParamsBuilder::new();
                let params = params_builder
                    .m_cost(config.ram_per_hasher)?
                    .p_cost(config.lanes)?
                    .data(&0_u64.to_be_bytes())?;
                let salt = SaltString::generate(rng);
                let mut salt_arr = [0u8; 64];
                let salt_bytes = salt.b64_decode(&mut salt_arr)?;
                let mut output = Vec::default();

                let minimum_duration = config.minimum_duration;
                let mut min_cost = 1;
                let mut total_spent_t = 0;
                let mut total_duration = Duration::ZERO;

                loop {
                    let t_cost = if total_spent_t > 0 {
                        let average_duration_per_t = total_duration / total_spent_t;
                        u32::try_from(ceil_divide(
                            minimum_duration.as_nanos(),
                            average_duration_per_t.as_nanos(),
                        ))
                        .unwrap()
                        .max(min_cost)
                    } else {
                        min_cost
                    };
                    params.t_cost(t_cost)?;

                    let params = params.clone().params()?;
                    self.allocate_blocks(&params);
                    let output_len = params
                        .output_len()
                        .unwrap_or(argon2::Params::DEFAULT_OUTPUT_LEN);
                    output.resize(output_len, 0);

                    let start = Instant::now();
                    let argon = Argon2::new(self.algorithm, Version::V0x13, params);
                    argon.hash_password_into_with_memory(
                        b"hunter2",
                        salt_bytes,
                        &mut output[..],
                        &mut self.blocks,
                    )?;
                    let elapsed = match Instant::now().checked_duration_since(start) {
                        Some(elapsed) => elapsed,
                        None => continue,
                    };
                    if elapsed < minimum_duration {
                        total_spent_t += t_cost;
                        total_duration += elapsed;
                        min_cost = t_cost + 1;
                    } else {
                        // TODO if it's too far past the minimum duration, maybe we should try again at a smaller cost?
                        break;
                    }
                }
                Ok(params_builder)
            }
        }
    }

    fn process_requests(mut self) {
        let mut rng = thread_rng();
        while let Ok(request) = self.receiver.recv() {
            let result = if let Some(verify_against) = &request.verify_against {
                Self::verify(&request, verify_against)
            } else {
                self.hash(&request, &mut rng)
            };
            drop(request.result_sender.send(result));
        }
    }

    fn verify(request: &HashRequest, hash: &str) -> Result<HashResponse, Error> {
        let hash = PasswordHash::new(hash)?;

        let algorithm = Algorithm::try_from(hash.algorithm)?;
        let version = hash
            .version
            .map(Version::try_from)
            .transpose()?
            .unwrap_or(Version::V0x13);

        let argon = Argon2::new(algorithm, version, argon2::Params::try_from(&hash)?);

        hash.verify_password(&[&argon], request.password.0.as_bytes())
            .map(|_| HashResponse::Verified)
            .map_err(Error::from)
    }

    fn hash<R: Rng + CryptoRng>(
        &mut self,
        request: &HashRequest,
        rng: &mut R,
    ) -> Result<HashResponse, Error> {
        let builder_template = self.builder_template.clone();
        let mut params =
            match builder_template.get_or_init(|| self.initialize_builder_template(rng)) {
                Ok(template) => template.clone(),
                Err(error) => return Err(Error::PasswordHash(error.to_string())),
            };

        params.data(&request.id.to_be_bytes())?;

        let params = params.params()?;
        self.allocate_blocks(&params);

        let salt = SaltString::generate(rng);
        let mut salt_arr = [0u8; 64];
        let salt_bytes = salt.b64_decode(&mut salt_arr)?;

        let argon = Argon2::new(self.algorithm, Version::V0x13, params);

        let output_len = argon
            .params()
            .output_len()
            .unwrap_or(argon2::Params::DEFAULT_OUTPUT_LEN);
        let output = argon2::password_hash::Output::init_with(output_len, |out| {
            Ok(argon.hash_password_into_with_memory(
                request.password.as_bytes(),
                salt_bytes,
                out,
                &mut self.blocks,
            )?)
        })?;

        Ok(HashResponse::Hash(SensitiveString(
            PasswordHash {
                algorithm: self.algorithm.ident(),
                version: Some(Version::V0x13.into()),
                params: ParamsString::try_from(argon.params())?,
                salt: Some(salt.as_salt()),
                hash: Some(output),
            }
            .to_string(),
        )))
    }

    fn allocate_blocks(&mut self, params: &argon2::Params) {
        for _ in self.blocks.len()..params.block_count() {
            self.blocks.push(Block::default());
        }
    }
}

#[derive(Debug)]
pub struct HashRequest {
    id: u64,
    password: SensitiveString,
    verify_against: Option<SensitiveString>,
    result_sender: flume::Sender<Result<HashResponse, Error>>,
}

#[derive(Debug)]
pub enum HashResponse {
    Hash(SensitiveString),
    Verified,
}

#[derive(thiserror::Error, Debug)]
enum ArgonError {
    #[error("{0}")]
    Argon(#[from] argon2::Error),
    #[error("{0}")]
    Hash(#[from] argon2::password_hash::Error),
}

fn ceil_divide(dividend: u128, divisor: u128) -> u128 {
    match divisor {
        0 => panic!("divide by 0"),
        1 => dividend,
        _ => {
            let rounding = divisor - 1;
            (dividend + rounding) / divisor
        }
    }
}

#[test]
fn basic_test() {
    use crate::config::SystemDefault;
    let hasher = Hasher::new(ArgonConfiguration::default());

    let password = SensitiveString(String::from("hunter2"));
    let hash = hasher.hash(1, password.clone()).unwrap();
    hasher.verify(1, password, hash).unwrap();

    let Hasher { sender, threads } = hasher;
    drop(sender);
    for thread in threads {
        thread.join().unwrap();
    }
}
