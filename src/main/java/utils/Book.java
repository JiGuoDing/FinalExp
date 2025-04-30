package utils;

// 保存 book 信息的辅助内部类
public class Book{
    String book_id;
    String goodreads_book_id;
    String best_book_id;
    String work_id;
    String authors;
    String original_publication_decade;
    String title;

    public String getBook_id() {
        return book_id;
    }

    public void setBook_id(String book_id) {
        this.book_id = book_id;
    }

    public String getGoodreads_book_id() {
        return goodreads_book_id;
    }

    public void setGoodreads_book_id(String goodreads_book_id) {
        this.goodreads_book_id = goodreads_book_id;
    }

    public String getBest_book_id() {
        return best_book_id;
    }

    public void setBest_book_id(String best_book_id) {
        this.best_book_id = best_book_id;
    }

    public String getWork_id() {
        return work_id;
    }

    public void setWork_id(String work_id) {
        this.work_id = work_id;
    }

    public String getAuthors() {
        return authors;
    }

    public void setAuthors(String authors) {
        this.authors = authors;
    }

    public String getOriginal_publication_decade() {
        return original_publication_decade;
    }

    public void setOriginal_publication_decade(String original_publication_decade) {
        this.original_publication_decade = original_publication_decade;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Book(String book_id, String goodreads_book_id, String best_book_id, String work_id, String authors, String original_publication_decade, String title) {
        this.book_id = book_id;
        this.goodreads_book_id = goodreads_book_id;
        this.best_book_id = best_book_id;
        this.work_id = work_id;
        this.authors = authors;
        this.original_publication_decade = original_publication_decade;
        this.title = title;
    }

    @Override
    public String toString() {
        return "Book{" +
                "book_id='" + book_id + '\'' +
                ", goodreads_book_id='" + goodreads_book_id + '\'' +
                ", best_book_id='" + best_book_id + '\'' +
                ", work_id='" + work_id + '\'' +
                ", authors='" + authors + '\'' +
                ", original_publication_decade='" + original_publication_decade + '\'' +
                ", title='" + title + '\'' +
                '}';
    }
}
