package wethinkcode.spa;

/*
 ** DO NOT CHANGE!!
 */


import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

public class MainPage extends AbstractPage {

    public MainPage(WebTestRunner testRunner) {
        super(testRunner);
    }

    @Override
    public String path() {
        return "/";
    }

    public void clickOn(String id) {
        click(By.id(id));
    }

    public void clickOnDictionaryLink() {
        clickOn("dictionary");
    }
    public void clickOnSynonymsLink() {
        clickOn("synonyms");
    }

    public void clickOnAntonymsLink() {
        clickOn("antonyms");
    }

    public MainPage fillWord(String word) {
        fillText(By.id("lookup"), word);
        return this;
    }

    public boolean assertAppDivElement() {
        return textOf(By.id("app")).equals("Click on a menu item.");
    }

    public void assertDictionaryMenuElement() {
        var links = driver.findElements(By.tagName("a"));
        assertThat(links.stream().map(WebElement::getText).anyMatch(p -> p.equals("Dictionary"))).isTrue();
    }

    public void assertLookupFormElement() {
        var form = driver.findElements(By.id("lookup-form"));
        assertThat(form).isNotNull();
    }

    public void assertWordElement(String word) {
        var el = new WebDriverWait(driver, Duration.ofSeconds(5))
                .until(ExpectedConditions.presenceOfElementLocated(By.id("word")));
        assertThat(el.getText()).isEqualTo(word);
    }

    public void assertPartOfSpeechElement(String partOfSpeech) {
        var elements = new WebDriverWait(driver, Duration.ofSeconds(5))
                .until(ExpectedConditions.presenceOfAllElementsLocatedBy(By.className("part-of-speech")));
        assertThat(elements.stream().anyMatch(e -> e.getText().equals(partOfSpeech))).isTrue();
    }

    public void assertDefinitionElements() {
        var elements = new WebDriverWait(driver, Duration.ofSeconds(5))
                .until(ExpectedConditions.presenceOfAllElementsLocatedBy(By.className("definition")));
        assertThat(elements).isNotEmpty();
    }

    void assertPhoneticElement(String phonetic) {
        var el = new WebDriverWait(driver, Duration.ofSeconds(5))
                .until(ExpectedConditions.presenceOfElementLocated(By.id("phonetic")));
        assertThat(el.getText()).isEqualTo(phonetic);
    }

    void assertAudioElement() {
        var el = new WebDriverWait(driver, Duration.ofSeconds(5))
                .until(ExpectedConditions.presenceOfElementLocated(By.tagName("audio")));
        assertThat(el.getAttribute("src")).isNotNull();
    }

    void assertExampleElements() {
        var elements = new WebDriverWait(driver, Duration.ofSeconds(5))
                .until(ExpectedConditions.presenceOfAllElementsLocatedBy(By.className("example")));
        assertThat(elements).isNotEmpty();
    }

    public void assertRelatedWordElements() {
        var elements = new WebDriverWait(driver, Duration.ofSeconds(5))
                .until(ExpectedConditions.presenceOfAllElementsLocatedBy(By.className("related-word")));
        assertThat(elements).isNotEmpty();
    }
}
