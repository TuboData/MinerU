from six import BytesIO

from docker.tubo_marker_pdf import MarkerPdf


def test():
    file = open('../demo1.pdf', 'rb')

    md = MarkerPdf._doHandle(BytesIO(file.read()))
    print(md)

if __name__ == '__main__':
    test()