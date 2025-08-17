from flask import Flask, render_template
from routes.query_routes import query_bp

app = Flask(__name__, template_folder="templates")

# Register blueprint
app.register_blueprint(query_bp)

@app.route("/")
def home():
    return render_template("index.html")
@app.route("/help")
def help_page():
    return render_template("help.html")


if __name__ == "__main__":
    app.run(debug=True)
