package wethinkcode.spa;

import static org.assertj.core.api.Assertions.assertThat;

class WebSession {
    protected final WebTestRunner testRunner;
    protected MainPage mainPage;

    /**
     * Setup the session.
     *
     * @param testRunner The test runner for this session
     */
    public WebSession(WebTestRunner testRunner) {
        this.testRunner = testRunner;
        mainPage = new MainPage(testRunner);
    }

    /**
     * Open a page
     *
     * @param page The page to open
     * @return This web session
     */
    public WebSession open(AbstractPage page) {
        testRunner.navigateTo(page);
        return this;
    }

    /**
     * Open the login page
     *
     * @return this web session
     */
    public WebSession openMainPage() {
        mainPage.open();
        return this;
    }

    /**
     * Check that the browser is on the login page
     *
     * @return this web session
     */
    public WebSession shouldBeOnMainPage() {
        testRunner.shouldBeOnPage(mainPage);
        return this;
    }

    public WebSession shouldHaveText(String string) {
        assertThat(mainPage.assertAppDivElement()).isTrue();
        return this;
    }

    public WebSession shouldHaveDictionaryMenu() {
        mainPage.assertDictionaryMenuElement();
        return this;
    }

    public WebSession clickOnDictionaryMenu() {
        mainPage.clickOnDictionaryLink();
        return this;
    }

    public WebSession shouldHaveLookupForm() {
        mainPage.assertLookupFormElement();
        return this;
    }

    public WebSession lookupWord(String word) {
        mainPage.fillWord(word).submit();
        return this;
    }

    public WebSession shouldHaveWord(String word) {
        mainPage.assertWordElement(word);
        return this;
    }

    public WebSession shouldHavePartOfSpeech(String partOfSpeech) {
        mainPage.assertPartOfSpeechElement(partOfSpeech);
        return this;
    }

    public WebSession shouldHaveDefinitions() {
        mainPage.assertDefinitionElements();
        return this;
    }

    public WebSession shouldHavePhonetic(String phonetic) {
        mainPage.assertPhoneticElement(phonetic);
        return this;
    }

    public WebSession shouldHaveAudio() {
        mainPage.assertAudioElement();
        return this;
    }

    public WebSession shouldHaveExamples() {
        mainPage.assertExampleElements();
        return this;
    }

    public WebSession shouldHaveRelatedWords() {
        mainPage.assertRelatedWordElements();
        return this;
    }

    public WebSession clickOnSynonymsMenu() {
        mainPage.clickOnSynonymsLink();
        return this;
    }

    public WebSession clickOnAntonymsMenu() {
        mainPage.clickOnAntonymsLink();
        return this;
    }
}
