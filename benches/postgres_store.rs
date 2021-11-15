use sparse_merkle_tree::{
    error::Error,
    merge::MergeValue,
    traits::{Hasher, Store},
    tree::{BranchKey, BranchNode},
    H256,
};
use std::cell::RefCell;
use std::marker::PhantomData;

use postgres::{Client, NoTls, Statement};

fn convert_postgres_error(error: postgres::error::Error) -> Error {
    Error::Store(error.to_string())
}

pub struct PostgresStore<H: Hasher + Default> {
    client: RefCell<Client>,
    get_branch_statement: Statement,
    get_leaf_statement: Statement,
    insert_branch_statement: Statement,
    insert_leaf_statement: Statement,
    remove_branch_statement: Statement,
    remove_leaf_statement: Statement,
    _phantom: PhantomData<H>,
}

fn new_postgres_store<H: Hasher + Default>() -> Result<PostgresStore<H>, postgres::error::Error> {
    let mut client = Client::connect(
        "host=127.0.0.1 port=5432 user=postgres password=123456 dbname=smt_benchmark",
        NoTls,
    )?;

    client.batch_execute("DROP TABLE IF EXISTS leaves_map")?;
    client.batch_execute("DROP TABLE IF EXISTS branches_map")?;

    client.batch_execute(
        "CREATE TABLE leaves_map (
                key     BYTEA PRIMARY KEY,
                value   BYTEA NOT NULL
            )",
    )?;

    client.batch_execute(
        "CREATE TABLE branches_map (
                height      INT NOT NULL,
                node_key    BYTEA NOT NULL,
                left_node   BYTEA NOT NULL,
                right_node  BYTEA NOT NULL,
                PRIMARY KEY(height, node_key)
            )",
    )?;

    let get_branch_statement = client.prepare(
        "SELECT left_node, right_node FROM branches_map WHERE height = $1 AND node_key = $2",
    )?;
    let get_leaf_statement = client.prepare("SELECT value FROM leaves_map WHERE key = $1")?;
    let insert_branch_statement = client.prepare(
        "INSERT INTO branches_map (
                height,
                node_key,
                left_node,
                right_node
            ) VALUES ($1, $2, $3, $4)
            ON CONFLICT(height, node_key) DO
            UPDATE SET left_node = $3, right_node = $4",
    )?;
    let insert_leaf_statement =
        client.prepare("INSERT INTO leaves_map (key, value) VALUES ($1, $2)")?;
    let remove_branch_statement =
        client.prepare("DELETE FROM branches_map WHERE height = $1 AND node_key = $2")?;
    let remove_leaf_statement = client.prepare("DELETE FROM leaves_map WHERE key = $1")?;

    Ok(PostgresStore {
        client: RefCell::new(client),
        get_branch_statement,
        get_leaf_statement,
        insert_branch_statement,
        insert_leaf_statement,
        remove_branch_statement,
        remove_leaf_statement,
        _phantom: PhantomData,
    })
}

impl<H: Hasher + Default> PostgresStore<H> {
    pub fn new() -> Result<Self, Error> {
        new_postgres_store().map_err(convert_postgres_error)
    }
}

impl<H: Hasher + Default> Default for PostgresStore<H> {
    fn default() -> Self {
        Self::new().unwrap()
    }
}

impl<H: Hasher + Default> Store<H256> for PostgresStore<H> {
    fn get_branch(&self, branch_key: &BranchKey) -> Result<Option<BranchNode>, Error> {
        let row = self
            .client
            .borrow_mut()
            .query_opt(
                &self.get_branch_statement,
                &[
                    &i32::from(branch_key.height),
                    &branch_key.node_key.as_slice(),
                ],
            )
            .map_err(convert_postgres_error)?;

        match row {
            None => Ok(None),
            Some(row) => {
                let mut raw_left: [u8; 32] = [0; 32];
                let mut raw_right: [u8; 32] = [0; 32];
                raw_left.copy_from_slice(row.get(0));
                raw_right.copy_from_slice(row.get(1));

                let left = H256::from(raw_left);
                let right = H256::from(raw_right);

                Ok(Some(BranchNode {
                    left: MergeValue::from_h256(left),
                    right: MergeValue::from_h256(right),
                }))
            }
        }
    }
    fn get_leaf(&self, leaf_key: &H256) -> Result<Option<H256>, Error> {
        let row = self
            .client
            .borrow_mut()
            .query_opt(&self.get_leaf_statement, &[&leaf_key.as_slice()])
            .map_err(convert_postgres_error)?;

        match row {
            None => Ok(None),
            Some(row) => {
                let mut raw_value: [u8; 32] = [0; 32];
                raw_value.copy_from_slice(row.get(0));

                let value: H256 = H256::from(raw_value);

                Ok(Some(value))
            }
        }
    }
    fn insert_branch(&mut self, branch_key: BranchKey, branch: BranchNode) -> Result<(), Error> {
        self.client
            .borrow_mut()
            .execute(
                &self.insert_branch_statement,
                &[
                    &i32::from(branch_key.height),
                    &branch_key.node_key.as_slice(),
                    &branch.left.hash::<H>().as_slice(),
                    &branch.right.hash::<H>().as_slice(),
                ],
            )
            .map_err(convert_postgres_error)?;
        Ok(())
    }
    fn insert_leaf(&mut self, leaf_key: H256, leaf: H256) -> Result<(), Error> {
        self.client
            .borrow_mut()
            .execute(
                &self.insert_leaf_statement,
                &[&leaf_key.as_slice(), &leaf.as_slice()],
            )
            .map_err(convert_postgres_error)?;
        Ok(())
    }
    fn remove_branch(&mut self, branch_key: &BranchKey) -> Result<(), Error> {
        self.client
            .borrow_mut()
            .execute(
                &self.remove_branch_statement,
                &[
                    &i32::from(branch_key.height),
                    &branch_key.node_key.as_slice(),
                ],
            )
            .map_err(convert_postgres_error)?;
        Ok(())
    }
    fn remove_leaf(&mut self, leaf_key: &H256) -> Result<(), Error> {
        self.client
            .borrow_mut()
            .execute(&self.remove_leaf_statement, &[&leaf_key.as_slice()])
            .map_err(convert_postgres_error)?;
        Ok(())
    }
}
