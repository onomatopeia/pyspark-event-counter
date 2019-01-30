import os


def rewrite_csv(input_file, output_file):
    with open(input_file, 'r', newline='') as csvin:
        with open(output_file, 'a', newline='') as csvout:
            for idx, line in enumerate(csvin):
                if idx > 0:  # ignore header
                    csvout.write(line)


def merge_output_files(output_directory, output_file, csv_header):
    with open(output_file, 'w+') as csvout:
        csvout.write(csv_header)
    for file in os.listdir(output_directory):
        if file.endswith('.csv'):
            rewrite_csv(os.path.join(output_directory, file), output_file)


def delete_folder(folder):
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(e)
    try:
        os.rmdir(folder)
    except Exception as e:
        print("Could not remove folder. \n{}".format(e))
