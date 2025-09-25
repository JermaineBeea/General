window.addEventListener("load", () => {
    const options = {
        method: 'GET',
    };

    const word = 'code';

    fetch(`https://api.dictionaryapi.dev/api/v2/entries/en/${word}`, options)
        .then(response => response.json())
        .then(data => {
            data = {
                word: data[0].word,
                phonetic: data[0].phonetic,
                partOfSpeech: data[0].meanings[0].partOfSpeech,
                definitions: data[0].meanings[0].definitions
            };
            const template = document.getElementById('template').innerText;
            const compiledFunction = Handlebars.compile(template);
            document.getElementById('app').innerHTML = compiledFunction(data);
        });
});