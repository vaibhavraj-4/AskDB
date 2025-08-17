# DBInsight

**DBInsight** is a Python-based web application designed to provide intelligent insights into SQL database schemas. It extracts metadata from databases to generate comprehensive documentation, aiding developers and database administrators in understanding and managing database structures effectively.

## Features

- **Schema Documentation**: Automatically generates detailed documentation of database schemas, including tables, views, indexes, and relationships.
- **Metadata Extraction**: Securely extracts metadata from SQL databases without accessing actual data records.
- **User Interface**: Provides an interactive web interface for exploring and navigating the generated documentation.
- **Role-Based Access Control**: Implements a secure user registration system with roles such as Owner, Admin, and User to manage access and permissions.

## Technologies Used

- **Backend**: Python
- **Web Framework**: Flask
- **Frontend**: HTML, CSS, JavaScript
- **Database**: SQLite (for application data storage)
- **Authentication**: Flask-Login for session management

## Installation

### Prerequisites

Ensure you have the following installed:

- Python 3.8 or higher
- pip (Python package installer)

### Steps

1. Clone the repository:

   ```bash
   git clone https://github.com/vaibhavraj-4/DBInsight.git
   cd DBInsight
   ```

2. Create and activate a virtual environment:

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
   ```

3. Install the required dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. Run the application:

   ```bash
   python app.py
   ```

The application will start, and you can access it in your web browser at [http://127.0.0.1:5000](http://127.0.0.1:5000).

## Usage

- Upon accessing the application, you'll be prompted to register or log in.
- After logging in, you can upload your SQL database credentials to extract metadata.
- The application will generate and display documentation of your database schema.
- Navigate through the documentation using the provided interface.

## Screenshots

Here are some screenshots of DBInsight in action:

**DBInsight Flow Diagram**  
![DBInsight Flow Diagram](static/screenshot/DBinsight_flow.webp)

**App Home Page**  
![Screenshot](static/screenshot/image2.png)

**Supported DBs'**  
![Screenshot](static/screenshot/image.png)

## Contributing

Contributions are welcome! To contribute:

1. Fork the repository.
2. Create a new branch (`git checkout -b feature-branch`).
3. Commit your changes (`git commit -am 'Add new feature'`).
4. Push to the branch (`git push origin feature-branch`).
5. Create a new Pull Request.

Please ensure your code adheres to the existing style and includes appropriate tests.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
