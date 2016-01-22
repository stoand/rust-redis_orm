extern crate rustc_serialize;
extern crate redis;
extern crate bincode;
extern crate uuid;

use rustc_serialize::{Decodable, Encodable};
use std::convert::Into;
use redis::{Commands, Pipeline, PipelineCommands};
use uuid::Uuid;

trait Database {
    fn store<T>(&self, record: &T) -> Result<Uuid, String> where T: Indexable;

    /// Create an index for a field in the database
    fn index_field<T>(&self, name: &'static str, value: &T) where T: Encodable;

    /// Numeric fields are indexed differently
    fn index_num_field<T>(&self, name: &'static str, value: &T) where T: Encodable + Into<f64>;
}

impl Database for redis::Connection {
    /// Writes indexable record to database and returns it's Uuid
    fn store<T>(&self, record: &T) -> Result<Uuid, String>
        where T: Indexable
    {
        // Make error.description() function available
        use std::error::Error;

        // Serialize the record into binary
        let encoded_record = try!(bincode::rustc_serialize::encode(record, bincode::SizeLimit::Infinite)
                                      .map_err(|encode_err| encode_err.description().to_string()));

        // Generate an id for the record
        let record_id = Uuid::new_v4();

        // Create the record
        try!(Pipeline::new()
                 .atomic()
                 .hset(T::get_storage_name(), record_id.as_bytes(), encoded_record)
                 .query(self)
                 .map_err(|redis_err: redis::RedisError| redis_err.description().to_string()));

        // Create record indices TODO
        record.index(self);

        Ok(record_id)
    }

    /// Create an index for a field in the database
    fn index_field<T>(&self, name: &'static str, value: &T)
        where T: Encodable
    {
        // TODO
    }

    /// Numeric fields are indexed differently
    fn index_num_field<T>(&self, name: &'static str, value: &T)
        where T: Encodable + Into<f64>
    {
        // TODO
    }
}

trait Indexable where Self: Encodable + Decodable + PartialEq {
    /// Creates indices for the record in the database that
    /// can later be used to search for it
    fn index<D>(&self, db: &D) where D: Database;

    /// Name of struct in database
    fn get_storage_name() -> &'static str;
}

#[derive(RustcEncodable, RustcDecodable, PartialEq)]
struct User {
    name: String,
    age: u16,
}

impl Indexable for User {
    fn index<D>(&self, db: &D)
        where D: Database
    {
        db.index_field("name", &self.name);
        db.index_num_field("age", &self.age);
    }

    fn get_storage_name() -> &'static str {
        "usr"
    }
}


fn main() {
    if let Ok(client) = redis::Client::open("redis://127.0.0.1") {
        let db = client.get_connection().unwrap();

        let user = User { name: "Andy".to_string(), age: 32 };

        println!("user id: {}", db.store(&user).unwrap());
    } else {
        panic!("Unable to connect to redis!");
    }
}
