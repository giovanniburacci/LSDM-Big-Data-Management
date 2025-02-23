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
    """Create a RediSearch index for efficient searching & aggregation."""
    try:
        redis_client.execute_command("FT.DROPINDEX", INDEX_NAME, "DD")  # Delete if exists
    except redis.exceptions.ResponseError:
        pass  # Ignore if index doesn't exist

    # Define the FT.CREATE command for RediSearch
    schema = ["FT.CREATE", INDEX_NAME, "ON", "HASH", "PREFIX", "1", "movie:",
              "SCHEMA"]

    # Add fields to schema
    for field in SEARCHABLE_FIELDS:
        schema.extend([field, "TEXT"])  # Searchable fields
    for field in NUMERIC_FIELDS:
        schema.extend([field, "NUMERIC", "SORTABLE"])  # Numeric and sortable

    # Execute command
    redis_client.execute_command(*schema)
    print("✅ Redis Search index created successfully!")

def load_csv_to_redis(csv_file):
    with open(csv_file, newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)

        pipeline = redis_client.pipeline()  # Use pipeline for efficiency

        for row in reader:
            movie_id = f"movie:{row['filmtv_id']}"  # Unique key for each movie

            # Prepare movie data
            movie_data = {k: v for k, v in row.items() if v.strip()}  # Remove empty fields

            # Convert numeric fields to proper types
            for field in NUMERIC_FIELDS:
                if field in movie_data:
                    try:
                        movie_data[field] = float(movie_data[field])  # Store as float for aggregations
                    except ValueError:
                        movie_data[field] = 0  # Default to 0 if parsing fails

            # Store movie in Redis as a HASH
            pipeline.hset(movie_id, mapping=movie_data)

        pipeline.execute()  # Execute all operations in batch
        print(f"✅ Loaded {reader.line_num - 1} movies into Redis.")

# Run the script
if __name__ == "__main__":
    csv_file_path = "filmtv_movies.csv"  # Change this to your CSV file path
    create_index()  # Create index before loading data
    load_csv_to_redis(csv_file_path)
