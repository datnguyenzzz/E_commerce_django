# Collaborative filtering "real-time" recommender system

User behaviors
  - Rating score
  - Click through
  - add to cart 
  - buy  

Endpoint:
  - :8765 - rest api 
  
  - :8761 - eureka service registry 

  - :8080 - storm ui 

  - :50070 - hadoop nodes

  - :8088 - hadoop resource manager 

  - :16010 - hbase

Recommendation architecture:

![alt text](https://raw.githubusercontent.com/datnguyenzzz/E_commerce_django/real-time-compute/assets/recommender-service.png)

Log aggregator architecture

![alt text](https://raw.githubusercontent.com/datnguyenzzz/Microservices_architecture/real-time-compute/assets/ELK_multiple_DCs.png)
