package edu.ecnu.touchstone.datatype;

import java.io.Serializable;
import java.util.*;

// In order to facilitate the data generation, the average length are converted to
// minimum length in the constructor.

public class TSVarchar implements TSDataTypeInfo {

    private static final long serialVersionUID = 1L;
    private static final char[] chars = ("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz").toCharArray();
    private float nullRatio;
    private int minLength;
    private int maxLength;
    private int cardinality;
    private String[] seeds = null;
    // support '=', 'in' operators
    private List<EqualCandidate> equalCandidates = null;
    // avoid generating the same random string
    private Set<String> equalCandidateSet = null;
    private float equalCumulativeProbability;
    // support 'like' operator
    private List<LikeCandidate> likeCandidates = null;
    // avoid generating the same random string
    private List<String> likeCandidateList = null;
    private float likeCumulativeProbability;

    public TSVarchar() {
        super();
        nullRatio = 0;
        minLength = 0;
        maxLength = 100;
        init();
    }

    public TSVarchar(float nullRatio, float avgLength, int maxLength, int cardinality) {
        super();
        this.cardinality = cardinality;
        this.nullRatio = nullRatio;
        if (maxLength / 2 > avgLength) {
            minLength = 0;
            this.maxLength = (int) (avgLength * 2);
        } else {
            minLength = (int) (2 * avgLength - maxLength);
            this.maxLength = maxLength;
        }
        init();
    }

    public TSVarchar(float nullRatio, float avgLength, int maxLength) {
        super();
        this.nullRatio = nullRatio;
        if (maxLength / 2 > avgLength) {
            minLength = 0;
            this.maxLength = (int) (avgLength * 2);
        } else {
            minLength = (int) (2 * avgLength - maxLength);
            this.maxLength = maxLength;
        }
        init();
    }

    public TSVarchar(TSVarchar tsVarchar) {
        super();
        this.nullRatio = tsVarchar.nullRatio;
        this.minLength = tsVarchar.minLength;
        this.maxLength = tsVarchar.maxLength;
        this.equalCandidates = new ArrayList<EqualCandidate>();
        for (int i = 0; i < tsVarchar.equalCandidates.size(); i++) {
            this.equalCandidates.add(new EqualCandidate(tsVarchar.equalCandidates.get(i)));
        }
        this.equalCandidateSet = new HashSet<String>();
        this.equalCandidateSet.addAll(tsVarchar.equalCandidateSet);
        this.equalCumulativeProbability = tsVarchar.equalCumulativeProbability;
        this.likeCandidates = new ArrayList<LikeCandidate>();
        for (int i = 0; i < tsVarchar.likeCandidates.size(); i++) {
            this.likeCandidates.add(new LikeCandidate(tsVarchar.likeCandidates.get(i)));
        }
        this.likeCandidateList = new ArrayList<String>();
        this.likeCandidateList.addAll(tsVarchar.likeCandidateList);
        this.likeCumulativeProbability = tsVarchar.likeCumulativeProbability;
        this.cardinality = tsVarchar.cardinality;
        if (tsVarchar.seeds != null) {
            this.seeds = Arrays.copyOf(tsVarchar.seeds, tsVarchar.seeds.length);
        }
    }

    private void init() {
        equalCandidates = new ArrayList<EqualCandidate>();
        equalCandidateSet = new HashSet<String>();
        equalCumulativeProbability = 0;
        likeCandidates = new ArrayList<LikeCandidate>();
        likeCandidateList = new ArrayList<String>();
        likeCumulativeProbability = 0;

        // seeds
        if (cardinality > 0) {
            seeds = new String[cardinality];
            for (int i = 0; i < seeds.length; i++) {
                seeds[i] = getRandomString();
            }
        }
    }

    @Override
    public String geneData() {
        double randomValue = Math.random();
        if (randomValue < nullRatio) {
            return null;
        } else if (randomValue < nullRatio + equalCumulativeProbability) {
            randomValue = Math.random() * equalCumulativeProbability;
            for (EqualCandidate equalCandidate : equalCandidates) {
                if (randomValue < equalCandidate.cumulativeProbability) {
                    return equalCandidate.candidate;
                }
            }
        } else if (randomValue < nullRatio + equalCumulativeProbability + likeCumulativeProbability) {
            randomValue = Math.random() * likeCumulativeProbability;
            for (LikeCandidate likeCandidate : likeCandidates) {
                if (randomValue < likeCandidate.cumulativeProbability) {
                    String frontStr = getRandomString(likeCandidate.frontLength);
                    String lastStr = getRandomString(likeCandidate.lastLength);
                    return frontStr + likeCandidate.candidate + lastStr;
                }
            }
        }
        String randomString;
        while (true) {
            if (cardinality > 0) {
                int randomIndex = (int) (Math.random() * cardinality - equalCandidates.size() - likeCandidates.size());
                randomString = seeds[randomIndex % seeds.length];
            } else {
                randomString = getRandomString();
            }
            boolean flag = true;
            for (String s : likeCandidateList) {
                if (randomString.contains(s)) {
                    flag = false;
                    break;
                }
            }
            if (flag && !equalCandidateSet.contains(randomString)) {
                break;
            }
        }
        return randomString;
    }

    // overall control needs to be done in call place for these two functions
    public String addEqualCandidate(float probability) {
        String candidate = getRandomString();
        while (equalCandidateSet.contains(candidate) || "".equals(candidate)) {
            candidate = getRandomString();
        }
        equalCumulativeProbability += probability;
        equalCandidates.add(new EqualCandidate(candidate, equalCumulativeProbability));
        equalCandidateSet.add(candidate);
        return candidate;
    }

    public String addLikeCandidate(float probability) {
        int frontLength, lastLength, candidateLength;
        // simplified implementation
        frontLength = 1;
        lastLength = 1;
        candidateLength = (int) (Math.random() * (maxLength - minLength + 1)) + minLength - 2;
        if (candidateLength <= 0) {
            candidateLength = 1;
        }

        String candidate = null;
        boolean flag = false;
        while (!flag) {
            String tmp = getRandomString(candidateLength);
            flag = equalCandidateSet.stream().noneMatch(x -> x.contains(tmp)) &&
                    likeCandidateList.stream().noneMatch(x -> x.contains(tmp));
            candidate = tmp;
        }

        likeCumulativeProbability += probability;
        likeCandidates.add(new LikeCandidate(candidate, likeCumulativeProbability, frontLength, lastLength));
        likeCandidateList.add(candidate);
        return candidate;
    }

    public void clear() {
        equalCandidates.clear();
        equalCandidateSet.clear();
        equalCumulativeProbability = 0;
        likeCandidates.clear();
        likeCandidateList.clear();
        likeCumulativeProbability = 0;
    }

    private String getRandomString() {
        int length = (int) (Math.random() * (maxLength - minLength + 1)) + minLength;
        return getRandomString(length);
    }

    private String getRandomString(int length) {
        char[] buffer = new char[length];
        for (int i = 0; i < length; i++) {
            buffer[i] = chars[(int) (Math.random() * 62)];
        }
        return new String(buffer);
    }

    @Override
    public String toString() {
        return "TSVarchar [nullRatio=" + nullRatio + ", minLength=" + minLength + ", maxLength=" + maxLength
                + ", equalCandidates=" + equalCandidates + ", equalCandidateSet=" + equalCandidateSet
                + ", equalCumulativeProbability=" + equalCumulativeProbability + ", likeCandidates=" + likeCandidates
                + ", likeCandidateList=" + likeCandidateList + ", likeCumulativeProbability="
                + likeCumulativeProbability + "]";
    }

    @Override
    public double getMinValue() {
        return 0;
    }

    @Override
    public double getMaxValue() {
        return 0;
    }
}

class EqualCandidate implements Serializable {

    private static final long serialVersionUID = 1L;

    String candidate = null;
    float cumulativeProbability;

    public EqualCandidate(String candidate, float cumulativeProbability) {
        super();
        this.candidate = candidate;
        this.cumulativeProbability = cumulativeProbability;
    }

    public EqualCandidate(EqualCandidate equalCandidate) {
        super();
        this.candidate = equalCandidate.candidate;
        this.cumulativeProbability = equalCandidate.cumulativeProbability;
    }

    @Override
    public String toString() {
        return "EqualCandidate [candidate=" + candidate + ", cumulativeProbability=" + cumulativeProbability + "]";
    }
}

class LikeCandidate implements Serializable {

    private static final long serialVersionUID = 1L;

    String candidate = null;
    float cumulativeProbability;
    int frontLength;
    int lastLength;

    public LikeCandidate(String candidate, float cumulativeProbability, int frontLength, int lastLength) {
        super();
        this.candidate = candidate;
        this.cumulativeProbability = cumulativeProbability;
        this.frontLength = frontLength;
        this.lastLength = lastLength;
    }

    public LikeCandidate(LikeCandidate likeCandidate) {
        super();
        this.candidate = likeCandidate.candidate;
        this.cumulativeProbability = likeCandidate.cumulativeProbability;
        this.frontLength = likeCandidate.frontLength;
        this.lastLength = likeCandidate.lastLength;
    }

    @Override
    public String toString() {
        return "LikeCandidate [candidate=" + candidate + ", cumulativeProbability=" + cumulativeProbability
                + ", frontLength=" + frontLength + ", lastLength=" + lastLength + "]";
    }
}