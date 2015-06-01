package fct.thesis.structures;

public class P<F extends Comparable<F>,S extends Comparable<S>> implements Comparable {
	public F f;
	public S s;
	
	public P(F f, S s) {
		this.f = f;
		this.s = s;
	}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof P)) return false;

        P p = (P) o;

        if (!f.equals(p.f)) return false;
        if (!s.equals(p.s)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = f.hashCode();
        result = 31 * result + s.hashCode();
        return result;
    }

    @Override
    public int compareTo(Object o) {
        P pO = (P) o;
        int n = this.f.compareTo((F) pO.f);
        return (n!=0)?n:s.compareTo((S) pO.s);
    }
}
