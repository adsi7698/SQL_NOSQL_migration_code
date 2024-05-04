CREATE TABLE movies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    year INT,
    rankscore FLOAT
);

CREATE TABLE roles (
    actor_id INT,
    movie_id INT,
    role VARCHAR(100),
    PRIMARY KEY (actor_id, movie_id, role),
    FOREIGN KEY (actor_id) REFERENCES actors(id),
    FOREIGN KEY (movie_id) REFERENCES movies(id)
);

