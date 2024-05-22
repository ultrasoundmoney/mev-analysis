CREATE TABLE transaction_blacklists (
  transaction_hash varchar(66) REFERENCES transactions (transaction_hash),
  blacklist_id blacklist_id_enum NOT NULL
);
