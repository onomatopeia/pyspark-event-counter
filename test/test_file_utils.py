from __future__ import print_function
from events_counter import delete_folder, merge_output_files
import os
import tempfile


def test_delete_folder():
    # prepare non-empty folder
    folder_name = tempfile.mkdtemp()
    print(folder_name)

    handle, file_path = tempfile.mkstemp(dir=folder_name)
    os.close(handle)

    # assert all ready
    assert os.path.isdir(folder_name), "Folder not created"
    assert os.path.exists(file_path), "File not created"

    # when
    delete_folder(folder_name)

    # then
    assert not os.path.isdir(folder_name), "Folder not deleted"
    assert not os.path.exists(os.path.join(folder_name, "test.txt")), "File not deleted"


def _sample_csv(folder, idx, n_lines, header):
    path_to_file = os.path.join(folder, 'file{}.csv'.format(idx))
    with open(path_to_file, 'w+') as f:
        f.write(header)
        for _ in range(n_lines):
            f.write("val1,val2\n")
    return path_to_file


def test_merge_files():
    # prepare files
    header = "col1,col2\n"
    result_file = 'result.csv'
    folder_name = tempfile.mkdtemp()
    print(folder_name)
    _sample_csv(folder_name, 1, 3, header)
    _sample_csv(folder_name, 2, 5, header)

    try:
        # when
        merge_output_files(folder_name, result_file, header)

        # then
        with open(result_file, 'r') as f:
            lines = f.readlines()
            assert len(lines) == 9, "Should be 9 lines, but was {}".format(len(lines))
            assert lines[0] == header, "First line should be a header but was {}".format(lines[0])

    finally:
        # cleanup
        if os.path.exists(result_file):
            os.unlink(result_file)
        delete_folder(folder_name)



