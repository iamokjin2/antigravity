package com.example.newssearch;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.util.List;

@RestController
@RequestMapping("/api/news")
@CrossOrigin(origins = "*")
public class NewsController {

    @Autowired
    private NewsRepository newsRepository;

    @GetMapping("/search")
    public List<NewsArticle> search(@RequestParam(value = "q", defaultValue = "") String query) {
        if (query.isEmpty()) {
            return newsRepository.findAll().stream().limit(50).toList();
        }
        return newsRepository.searchNews(query);
    }
    
    @GetMapping("/latest")
    public List<NewsArticle> latest() {
        return newsRepository.findLatest(20);
    }

    @GetMapping("/trending")
    public java.util.List<TrendingTopic> trending() {
        List<NewsArticle> recentNews = newsRepository.findLatest(100);
        java.util.Map<String, Integer> counts = new java.util.HashMap<>();
        
        for (NewsArticle article : recentNews) {
            String[] words = article.getTitle().split("\\s+");
            for (String word : words) {
                String cleanWord = word.replaceAll("[^가-힣a-zA-Z]", "");
                if (cleanWord.length() >= 2) {
                    counts.put(cleanWord, counts.getOrDefault(cleanWord, 0) + 1);
                }
            }
        }

        return counts.entrySet().stream()
                .sorted((a, b) -> b.getValue().compareTo(a.getValue()))
                .limit(10)
                .map(e -> new TrendingTopic(e.getKey(), e.getValue()))
                .toList();
    }

    public static record TrendingTopic(String keyword, int count) {}
}
