package com.example;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.codehaus.jettison.json.JSONException;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import com.google.gson.JsonObject;

/**
 * Created by sandesh on 2/24/16.
 */
public class JsonGenerator extends BaseOperator implements InputOperator {

    public final transient DefaultOutputPort<JsonObject> out = new DefaultOutputPort<JsonObject>();

    private int adsIdx = 0;
    private int eventsIdx = 0;
    private StringBuilder sb = new StringBuilder();
    private String pageID = UUID.randomUUID().toString();
    private String userID = UUID.randomUUID().toString();
    private final String[] eventTypes = new String[]{"view", "click", "purchase"};

    private List<String> ads;
    private final Map<String, List<String>> campaigns;

    public JsonGenerator() {
        this.campaigns = generateCampaigns();
        this.ads = flattenCampaigns();
    }

    public Map<String, List<String>> getCampaigns() {
        return campaigns;
    }

    /**
     * Generate a single element
     */
    public JsonObject generateElement()
    {
        JsonObject jsonObject = new JsonObject();
        try       {

        jsonObject.addProperty("user_id", userID);
        jsonObject.addProperty("page_id", pageID);

        if (adsIdx == ads.size()) {
            adsIdx = 0;
        }
        if (eventsIdx == eventTypes.length) {
            eventsIdx = 0;
        }

        jsonObject.addProperty("ad_id", ads.get(adsIdx++));
        jsonObject.addProperty("ad_type", "banner78");
        jsonObject.addProperty("event_type", eventTypes[eventsIdx++]);
        jsonObject.addProperty("event_time", System.currentTimeMillis());
        jsonObject.addProperty("ip_address", "1.2.3.4");
    }
    catch (Exception json){

    }

        return jsonObject;
    }

    /**
     * Generate a random list of ads and campaigns
     */
    private Map<String, List<String>> generateCampaigns() {
        int numCampaigns = 100;
        int numAdsPerCampaign = 10;
        Map<String, List<String>> adsByCampaign = new LinkedHashMap<>();
        for (int i = 0; i < numCampaigns; i++) {
            String campaign = UUID.randomUUID().toString();
            ArrayList<String> ads = new ArrayList<>();
            adsByCampaign.put(campaign, ads);
            for (int j = 0; j < numAdsPerCampaign; j++) {
                ads.add(UUID.randomUUID().toString());
            }
        }
        return adsByCampaign;
    }

    /**
     * Flatten into just ads
     */
    private List<String> flattenCampaigns() {
        // Flatten campaigns into simple list of ads
        List<String> ads = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : campaigns.entrySet()) {
            for (String ad : entry.getValue()) {
                ads.add(ad);
            }
        }
        return ads;
    }

    @Override
    public void emitTuples() {
        out.emit( generateElement() ) ;
    }
}
