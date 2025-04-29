package utils;

public class TagCnt {
    String tag_id;
    int cnt;

    public TagCnt(String tag_id, int cnt) {
        this.tag_id = tag_id;
        this.cnt = cnt;
    }

    public String getTag_id() {
        return tag_id;
    }

    public void setTag_id(String tag_id) {
        this.tag_id = tag_id;
    }

    public int getCnt() {
        return cnt;
    }

    public void setCnt(int cnt) {
        this.cnt = cnt;
    }
}
