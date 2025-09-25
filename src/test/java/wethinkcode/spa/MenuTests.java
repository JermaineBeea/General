package wethinkcode.spa;

import java.io.IOException;

/*
 ** DO NOT CHANGE!!
 */

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("When using the app")
public class MenuTests extends WebTestRunner {

    private static WebSession session;

    @BeforeEach
    public void setupSession() {
        session = new WebSession(this);
    }

    @Test
    @DisplayName("Main page has default text and dictionary menu")
    void visitMainPage() {
        session.open(new MainPage(MenuTests.this))
                .shouldBeOnMainPage()
                .shouldHaveText("Click on a menu item.")
                .shouldHaveDictionaryMenu();
    }

    @Test
    @DisplayName("Click on Dictionary menu")
    @Disabled
    void dictionaryMenu() {
        session.open(new MainPage(MenuTests.this))
                .shouldBeOnMainPage()
                .clickOnDictionaryMenu()
                .shouldHaveLookupForm();
    }

    @Test
    @DisplayName("Click on Synonyms menu")
    @Disabled
    void synonymsMenu() {
        session.open(new MainPage(MenuTests.this))
                .shouldBeOnMainPage()
                .clickOnSynonymsMenu()
                .shouldHaveLookupForm();
    }

    @Test
    @DisplayName("Click on Antonyms menu")
    @Disabled
    void antonymsMenu() {
        session.open(new MainPage(MenuTests.this))
                .shouldBeOnMainPage()
                .clickOnAntonymsMenu()
                .shouldHaveLookupForm();
    }
}
