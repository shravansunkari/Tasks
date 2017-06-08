import sys
from PIL import Image, ImageOps

PAT_SIZE = (8, 10)
NUMS = 13
FIRST_NUM_OFFSET = 5
NUM_OFFSET = (1, 5)


NUMBERS = []
for i in xrange(13):
    try:
        NUMBERS.append(Image.open('/home/srikanth/Desktop/imgs/%d.png' % i).load())
    except IOError:
        print "I do not know the pattern for the number %d." % i
        NUMBERS.append(None)


def magic(fname):
    captcha = ImageOps.grayscale(Image.open(fname))
    im = captcha.load()

    # Split numbers
    num = []
    for n in xrange(NUMS):
        x1, y1 = (FIRST_NUM_OFFSET + n * (NUM_OFFSET[0] + PAT_SIZE[0]),
                NUM_OFFSET[1])
        num.append(captcha.crop((x1, y1, x1 + PAT_SIZE[0], y1 + PAT_SIZE[1])))

    # If you want to save the split numbers:
    for i, n in enumerate(num):
        n.save('tmp%d.png' % i)
    print num

    def sqdiff(a, b):
        if None in (a, b): # XXX This is here just to handle missing pattern.
            return float('inf')

        d = 0
        for x in xrange(PAT_SIZE[0]):
            for y in xrange(PAT_SIZE[1]):
                d += (a[x, y] - b[x, y]) ** 2
        return d

    # Calculate a dummy sum of squared differences between the patterns
    # and each number. We assume the smallest diff is the number in the
    # "captcha".
    result = []
    for n in num:
        n_sqdiff = [(sqdiff(p, n.load()), i) for i, p in enumerate(NUMBERS)]
        result.append(min(n_sqdiff)[1])
    return result

print magic("/home/srikanth/Desktop/Cap_Img.jsp.jpeg")
#print NUMBERS
#a=magic("/home/srikanth/Desktop/Cap_Img.jsp.jpeg")
#sqdiff()
#print a
#print num