window.addEventListener("load", () => {
    const options = {
        method: 'GET',
    };

    const word = 'code';

    fetch(`https://api.dictionaryapi.dev/api/v2/entries/en/${word}`, options)
        .then(response => response.json())
        .then(data => {
            const content = data[0];
            const div = document.createElement('div')
            div.appendChild(createElementWithContent('h2', content.word + '&nbsp;' + content.phonetic));
            div.appendChild(createElementWithContent('h3', "Meanings"));
            div.appendChild(createElementWithContent('h4', content.meanings[0].partOfSpeech));
            div.appendChild(createListWithContent('ol', content.meanings[0].definitions, 'definition'));
            document.getElementById('app').replaceChildren(div);
        });


    function createElementWithContent(type, content) {
        const el = document.createElement(type);
        el.innerHTML = content;
        return el;
    }

    function createListWithContent(type, list, property) {
        const el = document.createElement(type);
        list.forEach(item => el.appendChild(createElementWithContent('li', item[property])));
        return el;
    }
});