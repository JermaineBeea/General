package wethinkcode.spa;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.junit.jupiter.api.*;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class that sets up the browser driver and has common user steps.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class WebTestRunner {
    protected Server server;
    protected String baseUrl;
    protected ChromeDriver driver;

    @BeforeAll
    void startServer() {
        server = new Server();
        server.start(0);
        baseUrl = "http://localhost:" + server.port();
    }

    @AfterEach
    void kill() {
        driver.quit();
    }

    @BeforeEach
    void start() {
        WebDriverManager.chromedriver().setup();
        ChromeOptions options = new ChromeOptions();
        options.addArguments("--no-sandbox"); // necessary for grading environment
        options.addArguments("--headless");
        options.addArguments("--disable-gpu");
        options.addArguments("--start-maximized");
        options.addArguments("--start-fullscreen");
        driver = new ChromeDriver(options);
    }

    @AfterAll
    void stopServer() {
        server.stop();
    }

    public WebDriver driver() {
        return driver;
    }

    /**
     * Tell the browser to go to a specific page
     *
     * @param page the page
     */
    public void navigateTo(AbstractPage page) {
        driver.get(appUrl(page.path()));
    }

    /**
     * Get the full URL for a page, including protocol, server, port, etc.
     *
     * @param pageUrl the url of the page
     * @return the full URL of the page
     */
    protected String appUrl(String pageUrl) {
        return baseUrl + pageUrl;
    }

    /**
     * Check that the browser is on the correct page
     *
     * @param page The expected page
     */
    public void shouldBeOnPage(AbstractPage page) {
        assertThat(currentPath()).isEqualToIgnoringCase(page.path());
    }

    /**
     * Get the path and query string from a full URI
     *
     * @param uri the URI to check
     * @return the path and query string of the URI
     */
    protected String pathAndQueryString(String uri) {
        try {
            URI currentUri = new URI(uri);
            if (Objects.isNull(currentUri.getQuery())) {
                return currentUri.getPath();
            } else {
                return currentUri.getPath() + "?" + currentUri.getQuery();
            }
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Could not parse [ " + driver.getCurrentUrl() + " ] to valid URI");
        }
    }

    private String currentPath() {
        return pathAndQueryString(driver.getCurrentUrl());
    }
}

