import csv
import redis

# Redis Connection
redis_client = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)

# Define the index name
INDEX_NAME = "movies_idx"

# Define searchable and sortable fields for RediSearch
SEARCHABLE_FIELDS = ["title", "genre", "country", "directors", "actors"]
NUMERIC_FIELDS = ["year", "duration", "avg_vote", "critics_vote", "public_vote", "total_votes",
                  "humor", "rhythm", "effort", "tension", "erotism"]

def create_index():
    try:
        # Delete if exists
        redis_client.execute_command("FT.DROPINDEX", INDEX_NAME, "DD")
    except redis.exceptions.ResponseError:
        # Ignore if index doesn't exist
        pass

    # Define the FT.CREATE command for RediSearch
    schema = ["FT.CREATE", INDEX_NAME, "ON", "HASH", "PREFIX", "1", "movie:",
              "SCHEMA"]

    # Add fields to schema
    for field in SEARCHABLE_FIELDS:
        # Searchable fields
        schema.extend([field, "TEXT"])
    for field in NUMERIC_FIELDS:
        # Numeric and sortable
        schema.extend([field, "NUMERIC", "SORTABLE"])

    # Execute command
    redis_client.execute_command(*schema)
    print("✅ Redis Search index created successfully!")

def load_csv_to_redis(csv_file):
    with open(csv_file, newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)

        # Use pipeline for efficiency
        pipeline = redis_client.pipeline()

        for row in reader:
            # Unique key for each movie
            movie_id = f"movie:{row['filmtv_id']}"

            # Prepare movie data
            movie_data = {k: v for k, v in row.items() if v.strip()}  # Remove empty fields

            # Convert numeric fields to proper types
            for field in NUMERIC_FIELDS:
                if field in movie_data:
                    try:
                        # Store as float for aggregations
                        movie_data[field] = float(movie_data[field])
                    except ValueError:
                        # Default to 0 if parsing fails
                        movie_data[field] = 0

            # Store movie in Redis as a HASH
            pipeline.hset(movie_id, mapping=movie_data)

        # Execute all operations in batch, solely for network optimization, not a transaction
        pipeline.execute()
        print(f"✅ Loaded {reader.line_num - 1} movies into Redis.")

if __name__ == "__main__":
    csv_file_path = "filmtv_movies.csv"
    create_index()
    load_csv_to_redis(csv_file_path)
