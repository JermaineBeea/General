package wethinkcode.spa;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Antonyms")
public class AntonymTests extends WebTestRunner {

    private static WebSession session;

    @BeforeEach
    public void setupSession() {
        session = new WebSession(this);
    }

    @Test
    @DisplayName("lookup a word")
    @Disabled
    void lookupHasEverything() {
        session.open(new MainPage(AntonymTests.this))
                .shouldBeOnMainPage()
                .clickOnAntonymsMenu()
                .lookupWord("hard")
                .shouldHaveWord("hard")
                .shouldHaveRelatedWords();
    }
}
