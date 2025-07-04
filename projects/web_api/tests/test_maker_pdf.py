from six import BytesIO

from busi.tubo_marker_doc.marker_doc import MarkerDoc


def test():
    file = open('../demo1.pdf', 'rb')

    md = MarkerDoc._do_handle(BytesIO(file.read()))
    print(md)

if __name__ == '__main__':
    test()