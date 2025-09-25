package wethinkcode.spa;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Dictionary")
public class DictionaryTests extends WebTestRunner {

    private static WebSession session;

    @BeforeEach
    public void setupSession() {
        session = new WebSession(this);
    }

    @Test
    @DisplayName("lookup a word")
    @Disabled
    void lookupHasEverything() {
        session.open(new MainPage(DictionaryTests.this))
                .shouldBeOnMainPage()
                .clickOnDictionaryMenu()
                .lookupWord("code")
                .shouldHaveWord("code")
                .shouldHavePhonetic("/kəʊd/")
                .shouldHaveAudio()
                .shouldHavePartOfSpeech("noun")
                .shouldHavePartOfSpeech("verb")
                .shouldHaveDefinitions()
                .shouldHaveExamples();
    }
}
