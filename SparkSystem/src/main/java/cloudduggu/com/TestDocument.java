package cloudduggu.com;

import java.io.Serializable;

/**
 * java bean class use to create, client string data into java object
 *
 * @author  Sarvesh Kumar (CloudDuggu.com)
 * @version 1.0
 * @since   2020-08-12
 */
@SuppressWarnings("serial")
public class TestDocument implements Serializable {

  private Integer id;
  private String text;

  public TestDocument(Integer id, String text) {
    this.id = id;
    this.text = text;
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

  public TestDocument() {
  }


}
