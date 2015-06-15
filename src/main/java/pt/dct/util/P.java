package pt.dct.util;

public class P<F,S> {
	public F f;
	public S s;
	
	public P(F f, S s) {
		this.f = f;
		this.s = s;
	}

    @Override
    public String toString() {
        return "P{" +
                "f=" + f +
                ", s=" + s +
                '}';
    }
}
