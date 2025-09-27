class Library:
    def __init__(self):
        self.books = ["1984", "To Kill a Mockingbird", "The Great Gatsby"]
    
    def list_books(self):
        print("Available books:")
        for book in self.books:
            print(f"- {book}")

    def add_books(self, book_name):
        if book_name not in self.books:
            self.books.append(book_name)
            print(f"'{book_name}' has been added to the library.")
        else:
            print(f"'{book_name}' is already in the library.")
    
    def remove_books(self, book_name):
        if book_name in self.books:
            self.books.remove(book_name)
            print(f"'{book_name}' has been removed from the library.")
        else:
            print(f"'{book_name}' is not in the library.")

user_1 = Library()
user_1.list_books()
user_1.add_books("Brave New World")
user_1.list_books()
