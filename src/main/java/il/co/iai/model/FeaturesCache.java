package il.co.iai.model;

import lombok.Data;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Data
public class FeaturesCache implements Serializable {

    private Map<String, Map<String, Object>> features = new ConcurrentHashMap<>();

    public void addSingleFeature(String eventId, String featureKey, Object featureValue) {
        if (features.containsKey(eventId)) {
            features.get(eventId).put(featureKey, featureValue);
        } else {
            Map<String, Object> newFeature = new ConcurrentHashMap<>();
            newFeature.put(featureKey, featureValue);
            features.put(eventId, newFeature);
        }
    }

    public void addSingleEventFeaturesMap(String eventId, Map<String, Object> map) {
        if (features.containsKey(eventId)) {
            for (String featureName : map.keySet()) {
                features.get(eventId).put(featureName, map.get(featureName));
            }
        } else {
            features.put(eventId, map);
        }
    }

    public Map<String, Object> getFeaturesByEventId(String eventId) {
        return features.get(eventId);
    }

    public void addFeatures(List<Map<String, Map<String, Object>>> featuresList) {
        for (Map<String, Map<String, Object>> eventFeatures : featuresList) {
            for (String eventId : eventFeatures.keySet()) {
                this.addSingleEventFeaturesMap(eventId, eventFeatures.get(eventId));
            }
        }
    }

    public void addFeatures(Map<String, Map<String, Object>> eventFeatures) {
        for (String eventId : eventFeatures.keySet()) {
            this.addSingleEventFeaturesMap(eventId, eventFeatures.get(eventId));
        }
    }


}
