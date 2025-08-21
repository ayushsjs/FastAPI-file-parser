# 📂 File Parser API  

A simple **FastAPI-based API** that allows parsing and processing uploaded files.  

---

## 🚀 Features  
- Built with **FastAPI** (Python)  
- REST API endpoints for file upload & parsing  
- Interactive API docs with **Swagger UI** (`/docs`) and **ReDoc** (`/redoc`)  
- Easy to extend for new file formats  

---

## ⚙️ Setup Instructions  

### 1. Clone the repository  
```bash
git clone https://github.com/your_username/file-parser-api.git
cd file-parser-api

2. Create a virtual environment
python -m venv venv

3. Activate the virtual environment
Windows (PowerShell):
venv\Scripts\activate

Linux / Mac:
source venv/bin/activate

4. Install dependencies
pip install -r requirements.txt

5. Run the FastAPI server
uvicorn main:app --reload


📘 API Documentation
1. Root Endpoint
GET /

Description: Health check / welcome message

Response (200 OK):

{
  "message": "Welcome to File Parser API 🚀"
}

2. Upload File Endpoint
POST /upload

Description: Upload a file to parse

Request: multipart/form-data

Request Parameters
Parameter	Type	Required	Description
file	File	Yes	File to upload (txt, csv, pdf etc.)

Example cURL Request
curl -X POST "http://127.0.0.1:8000/upload" \
  -H "accept: application/json" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@example.txt"

Example Response (200 OK)
  {
  "filename": "example.txt",
  "status": "File uploaded and parsed successfully"
}


🛠️ Tech Stack
Backend: FastAPI

Server: Uvicorn

Language: Python 3.10+



Author Name
Amit Yadav (https://github.com/amityadav0099)