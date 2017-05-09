package apoc.word.analyzer;

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;
import org.apache.commons.lang3.ArrayUtils;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by alberto.delazzari on 09/05/17.
 */
public class WordAnalyzer {

    private static final String TOKEN_MODEL_PATTERN = "model/%s-token.bin";

    private static final String SENTENCE_MODEL_PATTERN = "model/%s-sent.bin";

    private static final String FINDER_MODEL_PATTERN = "model/%s-ner-%s.bin";

    private TokenizerModel tokenizerModel;

    private Tokenizer tokenizer;

    private TokenNameFinderModel tokenNameFinderModel;

    private SentenceDetectorME sentenceFinder;

    private NameFinderME nameFinder;

    public WordAnalyzer(String language, String finderModel) throws Exception {
        InputStream tokenModelFileInputStream = ClassLoader.getSystemResourceAsStream(String.format(TOKEN_MODEL_PATTERN, language));
        System.out.println("TOKEN_MODEL_PATTERN = " + TOKEN_MODEL_PATTERN);
        System.out.println("tokenModelFileInputStream = " + tokenModelFileInputStream);
        tokenizerModel = new TokenizerModel(tokenModelFileInputStream);
        tokenModelFileInputStream.close();

        tokenizer = new TokenizerME(tokenizerModel);

        InputStream tokenFinderFileInputStream = ClassLoader.getSystemResourceAsStream(String.format(FINDER_MODEL_PATTERN, language, finderModel));
        tokenNameFinderModel = new TokenNameFinderModel(tokenFinderFileInputStream);
        tokenFinderFileInputStream.close();
        nameFinder = new NameFinderME(tokenNameFinderModel);

        InputStream sentenceModelFileInputStream = ClassLoader.getSystemResourceAsStream(String.format(SENTENCE_MODEL_PATTERN, language));
        SentenceModel sentenceModel = new SentenceModel(sentenceModelFileInputStream);
        sentenceModelFileInputStream.close();

        sentenceFinder = new SentenceDetectorME(sentenceModel);
    }

    public String[] getSentences(String inputText) {
        return sentenceFinder.sentDetect(inputText);
    }

    public String[] getTokens(String inputText) {
        return tokenizer.tokenize(inputText);
    }

    public String[] getNames(String[] tokens) {
        Span nameSpans[] = nameFinder.find(tokens);
        List<String> fullNames = new ArrayList<>();
        for (Span span : nameSpans) {
            String subTokens[] = ArrayUtils.subarray(tokens, span.getStart(), span.getEnd());

            String fullName = Stream.of(subTokens).collect(Collectors.joining(" "));
            fullNames.add(fullName);
        }

        return toArray(fullNames);
    }

    private String[] toArray(List<String> list) {
        String[] array = new String[list.size()];
        int i = 0;
        for (String s : list) {
            array[i] = s;
            i++;
        }

        return array;
    }
}