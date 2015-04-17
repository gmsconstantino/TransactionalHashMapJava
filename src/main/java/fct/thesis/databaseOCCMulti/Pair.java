package fct.thesis.databaseOCCMulti;

public class Pair<F,S> {
	public F f;
	public S s;

	public Pair(F f, S s) {
		this.f = f;
		this.s = s;
	}

    public Pair() {
        this.f = null;
        this.s = null;
    }

    @Override
    public String toString() {
        return "Pair{" +
                "f=" + f +
                ", s=" + s +
                '}';
    }
}
