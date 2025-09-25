window.addEventListener('load', router);
window.addEventListener('hashchange', router);

const routes = [
  { path: "/dictionary", handler: dictionaryRouteHandler },
  { path: "/thesaurus", handler: thesaurusRouteHandler }
];

const router = () => {
  const path = location.hash.slice(1).toLowerCase() || '/';
  const { handler = defaultRouteHandler } = routes.find(r => r.path == path) || {};
  handler();
};

const defaultRouteHandler = () => {
  document.getElementById('app').innerHTML = '<p>Click on a menu item.</p>';
};

const thesaurusRouteHandler = () => {
  document.getElementById('app').innerHTML = `
    <h1>Thesaurus</h1>
    <p>Coming soon!</p>
    <br/>`;
};

const dictionaryRouteHandler = () => {
  document.getElementById('app').innerHTML =  `
    <h1>Dictionary</h1>
    <br/>
    <form id="lookup-form"> 
      <input name="word" type="text" required/>
      <input name="submit" type="submit" value="Lookup"/>
    </form>
    <br/>
    <div id="results"></div>`;
  lookupWord();
};

function lookupWord() {
  const form = document.getElementById("lookup-form");
  form.addEventListener("submit", (event) => {
    event.preventDefault();

    const data = new FormData(event.target);
    const word = data.get("word");

    const options = {
      method: 'GET',
    };

    document.getElementById('results').innerHTML = `<p>Searching for <em>${word}</em>...</p>`;

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
        document.getElementById('results').innerHTML = compiledFunction(data);
      });
  });
}