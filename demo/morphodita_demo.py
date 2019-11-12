import sys

from ufal.morphodita import Tagger, Forms, TaggedLemmas, TokenRanges

def encode_entities(text):
  return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')


#if len(sys.argv) == 1:
#  sys.stderr.write('Usage: %s tagger_file\n' % sys.argv[0])
#  sys.exit(1)

sys.stderr.write('Loading tagger: ')
t = "../external/morphodita/czech-morfflex-pdt-161115-no_dia-pos_only.tagger"
#tagger = Tagger.load(sys.argv[1])
tagger = Tagger.load(t)
if not tagger:
  sys.stderr.write("Cannot load tagger from file '%s'\n" % t)
  sys.exit(1)
sys.stderr.write('done\n')

forms = Forms()
lemmas = TaggedLemmas()
tokens = TokenRanges()
tokenizer = tagger.newTokenizer()
if tokenizer is None:
  sys.stderr.write("No tokenizer is defined for the supplied model!")
  sys.exit(1)

not_eof = True
while not_eof:
  text = ''

  # Read block
  while True:
    line = sys.stdin.readline()
    not_eof = bool(line)
    if not not_eof: break
    line = line.rstrip('\r\n')
    text += line
    text += '\n';
    if not line: break



  # Tag
  tokenizer.setText(text)
  t = 0
  while tokenizer.nextSentence(forms, tokens):
    tagger.tag(forms, lemmas)

    for i in range(len(lemmas)):
      lemma = lemmas[i]
      token = tokens[i]
      sys.stdout.write('%s%s<token lemma="%s" tag="%s">%s</token>%s' % (
        encode_entities(text[t : token.start]),
        "<sentence>" if i == 0 else "",
        encode_entities(lemma.lemma),
        encode_entities(lemma.tag),
        encode_entities(text[token.start : token.start + token.length]),
        "</sentence>" if i + 1 == len(lemmas) else "",
      ))
      t = token.start + token.length
  sys.stdout.write(encode_entities(text[t : ]))