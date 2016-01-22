extern crate rustc_serialize;
extern crate redis;
extern crate bincode;
extern crate uuid;

use rustc_serialize::{Decodable, Encodable};
use bincode::rustc_serialize::EncodingResult;
use std::f64;
use redis::{Commands, ConnectionLike, Pipeline, PipelineCommands};
use uuid::Uuid;
use std::error::Error;

trait Indexable where Self: Encodable + Decodable + PartialEq {
    /// Writes indexable record to database and returns it's Uuid

    fn store(&self, db: &redis::Connection) -> Result<Uuid, String> {

        // Serialize the record into binary
        let encoded_record = try!(bincode::rustc_serialize::encode(self, bincode::SizeLimit::Infinite)
                                      .map_err(|encode_err| encode_err.description().to_string()));
        // Generate an id for the record
        let record_id = Uuid::new_v4();

        // Create redis pipeline which represents a transaction
        let mut pipeline = Pipeline::new();
        pipeline.atomic();

        // Save the record to the database
        pipeline.hset(Self::get_storage_name(), record_id.as_bytes(), encoded_record);

        let indices = try!(self.get_indices().map_err(|encode_err| encode_err.description().to_string()));

        for index in indices {
            // Unique identifier for this field
            let mut index_key = match index {
                Index::Raw{name, ..} | Index::Numeric  {name, ..} => {
                    (Self::get_storage_name().to_string() + "\n" + name + "\n").into_bytes()
                }
            };

            match index {
                Index::Raw{value, ..} => {
                    // Append the value to the unique field name
                    index_key.append(&mut value.clone());
                    // Add the record id to a set which keeps track of records
                    // of a certain type with a certain value
                    pipeline.sadd(index_key, record_id.as_bytes());
                }
                Index::Numeric {value, ..} => {
                    pipeline.zadd(index_key, record_id.as_bytes(), value);
                }
            };
        }

        // Run pipeline queries
        try!(pipeline.query(db).map_err(|redis_err| redis_err.description().to_string()));

        Ok(record_id)
    }
    /// Creates indices for the record in the database that
    /// can later be used to search for it
    fn get_indices(&self) -> EncodingResult<Vec<Index>>;

    /// Name of struct in database
    fn get_storage_name() -> &'static str;
}

enum Index {
    Raw {
        name: &'static str,
        value: Vec<u8>,
    },
    Numeric {
        name: &'static str,
        value: f64,
    },
}

#[derive(RustcEncodable, RustcDecodable, PartialEq)]
struct User {
    name: String,
    age: u16,
}

impl Indexable for User {
    fn get_indices(&self) -> EncodingResult<Vec<Index>> {
        Ok(vec![Index::Raw { name: "name", value: self.name.as_bytes().to_vec() }, Index::Numeric { name: "age", value: self.age as f64 }])
    }

    fn get_storage_name() -> &'static str {
        "usr"
    }
}


fn main() {
    if let Ok(client) = redis::Client::open("redis://127.0.0.1") {
        let db = client.get_connection().unwrap();

        let user = User { name: "Andy".to_string(), age: 32 };

        println!("user id: {}", user.store(&db).unwrap());
    } else {
        panic!("Unable to connect to redis!");
    }
}
