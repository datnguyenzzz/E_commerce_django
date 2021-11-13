* Payment service 
+ Akka, Kafka for streamming data 
* Responsibility
+ Learning stream data processing with Akka
+ Zookeeper for run kafka broker

+ Merchant account (payment processor) vs Payment services provider (payment gateway + payment processor) 
  -) Merchant account: Tight contract condition, need create own gateway. Don't need worry about service outage 
  -) PSP: Loose contract condition, fully rely on provider

 => Event using PSP, using need create own payment gateway. Which is route to mulitple provider, depend on specific
   conditions

+ Subjects - AS A PAYMENT SERVICE PROVIDER 
  . Create own payment processor: Stream payment info through VISA-card network to issuer bank
  . Create own payment gateway