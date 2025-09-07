package wethinkcode.httpapi;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.plugin.json.JsonMapper;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

/**
 * Exercise 1
 * <p>
 * Application Server for the Tasks API
 */
public class TasksAppServer {
    private static final TasksDatabase database = new TasksDatabase();

    private final Javalin appServer;

    /**
     * Create the application server and configure it.
     */
    public TasksAppServer() {
        this.appServer = Javalin.create(config -> {
            config.defaultContentType = "application/json";
            config.jsonMapper(createGsonMapper());
        });

          this.appServer.get("/tasks", this::getAllTasks);
        this.appServer.get("/task/{id}", this::getTaskById); 
        this.appServer.post("/task", this::addTask);

    }

        /**
     * Get a single task by id
     *
     * @param context the server context
     */
    private void getTaskById(Context context) {
        int id = Integer.parseInt(context.pathParam("id"));
        Task task = database.get(id);

        if (task == null) {
            context.status(HttpCode.NOT_FOUND);
            context.contentType("application/json");
            context.json("Task not found");
        } else {
            context.contentType("application/json");
            context.json(task);
        }
    }

        /**
     * Add a new task
     *
     * @param context the server context
     */
    private void addTask(Context context) {
        Task task = context.bodyAsClass(Task.class);

        boolean added = database.add(task);
        if (added) {
            context.status(HttpCode.CREATED);
            context.header("Location", "/task/" + task.getId());
        } else {
            // handled in 1.4
            context.status(HttpCode.BAD_REQUEST);
        }
    }


    /**
     * Use GSON for serialisation instead of Jackson
     * because GSON allows for serialisation of objects without noargs constructors.
     *
     * @return A JsonMapper for Javalin
     */
    private static JsonMapper createGsonMapper() {
        Gson gson = new GsonBuilder().create();
        return new JsonMapper() {
            @NotNull
            @Override
            public String toJsonString(@NotNull Object obj) {
                return gson.toJson(obj);
            }

            @NotNull
            @Override
            public <T> T fromJsonString(@NotNull String json, @NotNull Class<T> targetClass) {
                return gson.fromJson(json, targetClass);
            }
        };
    }

    /**
     * Start the application server
     *
     * @param port the port for the app server
     */
    public void start(int port) {
        this.appServer.start(port);
    }

    /**
     * Stop the application server
     */
    public void stop() {
        this.appServer.stop();
    }

    /**
     * Get all tasks
     *
     * @param context the server context
     */
    private void getAllTasks(Context context) {
        context.contentType("application/json");
        context.json(database.all());
    }
}
