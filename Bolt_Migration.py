####### FILL THESE ##############
config_hash = "COPY1_H:CUSTOMER_PROFILE:"
profileVersion = "ProfileVersion"

# Source and Destination Redis server configuration
src_server = {
    "host": "127.0.0.1", 
    "port": 6379,                
    "password": "radware123" 
}

dst_server = {
    "host": "127.0.0.1",  
    "port": 6379,                     
    "password": "radware123"
}

####### FILL END ##############

import sys
import redis

def copy_keys(src_redis, dst_redis, pattern):
    cursor = 0
    while True:
        cursor, keys = src_redis.scan(cursor=cursor, match=pattern)
        for key in keys:
            key_str = key.decode()
            print(f"Processing hash: {key_str}")

            # Retrieve all fields and values from the source hash
            all_fields = src_redis.hgetall(key)

            # Copy each field and value to the destination hash
            for field, value in all_fields.items():
                # Check if the field already exists in the destination hash
                # if not dst_redis.hexists(key_str, field):  
                    if field.decode().startswith("bolt"):
                        dst_redis.hset(key_str, field, value)
                        print(f"Field {field.decode()} copied to {key_str} in dest_redis.")
            print(f"Hash {key_str} copied to {key_str} in dest_redis.")

        if cursor == 0:
            break

def add_bid_fields(redis_instance, pattern):
    for key in redis_instance.scan_iter(match=pattern):
        key_str = key.decode()

        # Retrieve all fields and values from the hash
        all_fields = redis_instance.hgetall(key)

        isProfileUpdate = True 

        # Iterate through fields and create 'b*' versions for those starting with 'bolt'
        for field, value in all_fields.items():
            boltField = field.decode()
            boltValue = value.decode()
            if boltField.startswith("bolt"):
                if isProfileUpdate == True:
                    new_version = redis_instance.hincrby(key_str, profileVersion, 1)
                    isProfileUpdate = False
                    print(f"New Profile version {new_version} has been updated to {key_str}")
                b_field = "b" + boltField[4:]
                b_value = "1" if boltValue == "true" else "0"
                redis_instance.hset(key_str, b_field, b_value)

        print(f"Added b* fields to {key_str}.")

def main():
    # Connect to the source and destination Redis servers
    src_redis = redis.Redis(
        host=src_server['host'], 
        port=src_server['port'], 
        password=src_server['password']
    )
    dst_redis = redis.Redis(
        host=dst_server['host'], 
        port=dst_server['port'], 
        password=dst_server['password']
    )

    pattern = config_hash + "*"

    # Step 1: Copy keys from source to destination
    copy_keys(src_redis, dst_redis, pattern)

    # Step 2: Add 'b*' fields to config keys
    add_bid_fields(src_redis, pattern)

    # Step 3: Add 'b*' fields to mrm keys
    add_bid_fields(dst_redis, pattern)

if __name__ == "__main__":
    main()
