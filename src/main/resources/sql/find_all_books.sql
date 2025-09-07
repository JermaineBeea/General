SELECT Books.title, Genres.description
FROM Books
JOIN Genres ON Books.genre_code = Genres.code;
