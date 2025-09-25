window.addEventListener("load", () => {
    const options = {
        method: 'GET',
    };

    const word = 'code';

    fetch(`https://api.dictionaryapi.dev/api/v2/entries/en/${word}`, options)
        .then(response => response.json())
        .then(data => {
            const content = data[0];
            const meaning = content.meanings[0];

            const html =
            `
                <h2>${content.word}&nbsp;&nbsp;${content.phonetic}</h2>
                <h3>Meanings</h3>
                <h4>${meaning.partOfSpeech}</h4>
                <ol>
                    ${listOfDefinitions(meaning.definitions)}
                </ol>
            `;
            document.getElementById('app').innerHTML = html;
        });
});

function listOfDefinitions(definitions) {
    return definitions.reduce((previous, current) => previous + `<li>${current.definition}</li>`, '');
}