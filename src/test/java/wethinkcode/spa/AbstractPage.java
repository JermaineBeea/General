package wethinkcode.spa;

import java.util.function.Function;

/*
 ** DO NOT CHANGE!!
 */
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;


/**
 * Base class that represents a page in the app that is under test
 */
public abstract class AbstractPage {
    protected final WebTestRunner testRunner;
    protected final WebDriver driver;

    public AbstractPage(WebTestRunner testRunner) {
        this.testRunner = testRunner;
        this.driver = testRunner.driver();
    }

    protected Function<String, Function<String, By>> rowLocator() {
        return (id) -> (row) -> By.id(id + "_" + row);
    }

    public void open() {
        testRunner.navigateTo(this);
    }

    public abstract String path();

    public void fillText(By element, String keys) {
        var field = testRunner.driver().findElement(element);
        field.clear();
        field.sendKeys(keys);
    }

    public void click(By element) {
        testRunner.driver().findElement(element).click();
    }

    public void submit() {
        WebElement button = testRunner.driver().findElement(By.xpath("//input[@type='submit']"));
        button.submit();
    }

    public String textOf(By locator) {
        return driver.findElement(locator).getText();
    }

    protected String hrefOf(By locator) {
        return driver.findElement(locator).getAttribute("href");
    }
}
