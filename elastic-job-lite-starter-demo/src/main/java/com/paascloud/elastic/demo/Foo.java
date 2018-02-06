package com.paascloud.elastic.demo;

import lombok.Data;

/**
 * The class Foo.
 *
 * @author paascloud.net @gmail.com
 */
@Data
public class Foo {
	/**
	 * The Id.
	 */
	private Long id;

	/**
	 * Instantiates a new Foo.
	 *
	 * @param id the id
	 */
	public Foo(Long id) {
		this.id = id;
	}

}
