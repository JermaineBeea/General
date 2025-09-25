window.addEventListener("load", () => {
    const options = {
        method: 'GET',
    };

    const word = 'code';

    fetch(`https://api.dictionaryapi.dev/api/v2/entries/en/${word}`, options)
        .then(response => response.json())
        .then(data => console.log(JSON.stringify(data, null, 2)));
});