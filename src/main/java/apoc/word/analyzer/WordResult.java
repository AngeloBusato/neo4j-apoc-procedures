package apoc.word.analyzer;

import java.util.List;

/**
 * Created by larusba on 5/9/17.
 */

public class WordResult {

    public List<String> result;

    public WordResult(List<String> result) {
        this.result = result;
    }

    public List<String> getResult() {
        return result;
    }

    public void setResult(List<String> result) {
        this.result = result;
    }
}