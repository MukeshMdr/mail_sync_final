import docx
import PyPDF2
import os
# from utils import tar_pdf_dir, save_txt_dir, pdf_dir

def convert_docx_to_txt(docx_path, txt_path):
    doc = docx.Document(docx_path)
    with open(txt_path, 'w', encoding='utf-8') as txt_file:
        for paragraph in doc.paragraphs:
            txt_file.write(paragraph.text + '\n')

def convert_pdf_to_txt(pdf_path, txt_path):
    with open(pdf_path, 'rb') as pdf_file:
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        with open(txt_path, 'w', encoding='utf-8') as txt_file:
            for page_num in range(len(pdf_reader.pages)):
                page = pdf_reader.pages[page_num]
                txt_file.write(page.extract_text() + '\n')


def find_files(folder, extension):
    files = []
    for root, dirs, filenames in os.walk(folder):
        for filename in filenames:
            if filename.endswith(extension):
                files.append(os.path.join(root, filename))
    return files

# folder_path = pdf_dir
# file_extension = '.pdf' 

# found_files = find_files(folder_path, file_extension)

# if found_files:
#     print("Found files:")
#     for file in found_files:
#         print(file)
#         print(file.split(".")[0]+".txt")
#         convert_pdf_to_txt(file,file.split(".")[0]+".txt" )

# else:
#     print("No files found.")


# # convert_pdf_to_txt(tar_pdf_dir,save_txt_dir )
