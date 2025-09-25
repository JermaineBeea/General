window.addEventListener("load", () => {
    const form = document.getElementById("lookup-form");

    form.addEventListener("submit", (event) => {
        event.preventDefault();

        const data = new FormData(event.target);
        const word = data.get("word");

        const options = {
            method: 'GET',
        };

        document.getElementById('app').innerHTML = `<p>Searching for <em>${word}'</em>...</p>`;

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
});