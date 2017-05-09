package apoc.word.analyzer;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Created by alberto.delazzari on 09/05/17.
 */
public class WordAnalyzerProcedure {

    @Procedure
    @Description("CALL apoc.word.analyzer.sentences(text, language) yield sentences - returns all sentences for a given input text")
    public Stream<WordResult> sentences(@Name(value = "text") String text, @Name(value = "language", defaultValue = "en") String language) throws Exception {
        System.out.println("text = " + text);
        System.out.println("language = " + language);
        WordAnalyzer wordAnalyzer = new WordAnalyzer(language, "");
        String[] sentences = wordAnalyzer.getSentences(text);
        return Arrays.asList(sentences).stream().map(s -> new WordResult(s));
    }

    @Procedure
    @Description("CALL apoc.word.analyzer.tokens(text, language) yield tokens - returns all tokens for a given input text")
    public Stream<WordResult> tokens(@Name(value = "text") String text, @Name(value = "language", defaultValue = "en") String language) throws Exception {
        WordAnalyzer wordAnalyzer = new WordAnalyzer(language, "");
        String[] tokens = wordAnalyzer.getTokens(text);

        return Arrays.asList(tokens).stream().map(s -> new WordResult(s));
    }
}