Auto complete service

# Create topic
docker exec -it <image_name> kafka-topics.sh --create --bootstrap-server <INSIDE:kafka:9092 || OUTSIDE:localhost:9093> --topic <>
# Create events
docker exec -it <image_name> kafka-console-producer.sh --bootstrap-server <INSIDE:kafka:9092 || OUTSIDE:localhost:9093> --topic <>
# Read events
docker exec -it <image_name> kafka-console-consumer.sh --bootstrap-server <INSIDE:kafka:9092 || OUTSIDE:localhost:9093> --topic <>--from-beginning

APIs:

+ /search?prefix=... Search for top 5 common for prefix
+ /buildTree?search=... Add words to tree