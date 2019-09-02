USE Points;

CREATE TABLE points
(
  user_id BINARY(16) NOT NULL,
  total int UNSIGNED NOT NULL,
  total_temporary int UNSIGNED NOT NULL,
  payed_temporary int UNSIGNED NOT NULL DEFAULT 0,
  reserved int UNSIGNED NOT NULL DEFAULT 0,
  earliest_expiry_date datetime COMMENT 'utc',
  earliest_expiry_amount int UNSIGNED,
  PRIMARY KEY(user_id)
);

CREATE TABLE transaction
(
  id serial NOT NULL,
  user_id BINARY(16) NOT NULL,
  amount int NOT NULL,
  time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'utc',
  expiry_time datetime COMMENT 'utc',
  action ENUM("add user",
  			  "add points",
  			  "reserve",
  			  "committed",
  			  "canceled",
  			  "temporary points addition") NOT NULL COMMENT 'aka description',
  PRIMARY KEY(id)
);

CREATE TABLE temporary_points
(
  user_id BINARY(16) NOT NULL,
  transaction_id bigint UNSIGNED NOT NULL UNIQUE,
  amount int UNSIGNED NOT NULL,
  expiry_time datetime NOT NULL COMMENT 'utc',
  PRIMARY KEY(transaction_id)
);

ALTER TABLE transaction ADD FOREIGN KEY (`user_id`) REFERENCES `points` (`user_id`);

ALTER TABLE temporary_points ADD FOREIGN KEY (`user_id`) REFERENCES `points` (`user_id`);

ALTER TABLE temporary_points ADD FOREIGN KEY (`transaction_id`) REFERENCES `transaction` (`id`);


