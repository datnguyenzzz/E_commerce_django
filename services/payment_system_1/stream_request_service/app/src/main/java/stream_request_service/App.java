/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package stream_request_service;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.http.javadsl.Http; 
import akka.http.javadsl.ServerBinding; 
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;

import java.util.concurrent.CompletionStage;
import java.io.IOException;

public class App extends AllDirectives{

    private Route createRoute() {
        return concat(
            path("api", () -> 
                get(() -> complete("This is server for api....")))
        );
    }

    public static void main(String[] args) {
        ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(),"stream-request-server");
        
        final Http http = Http.get(system);
        App app = new App(); 

        final CompletionStage<ServerBinding> binding = 
            http.newServerAt("localhost",8000)
                .bind(app.createRoute());

        System.out.println("Server is online");
        try {
            System.in.read();
        }
        catch (IOException e) {
            System.out.println(e.getMessage());
        }

        binding
            .thenCompose(ServerBinding::unbind)
            .thenAccept(unbound -> system.terminate());

    }
}
